//! Smart caching and deduplication of requests when using a forking provider
use crate::executor::{
    backend::error::{DatabaseError, DatabaseResult},
    fork::{cache::FlushJsonBlockCacheDB, BlockchainDb},
};
use ethers::{
    core::abi::ethereum_types::BigEndianHash,
    providers::Middleware,
    types::{Address, Block, BlockId, Bytes, Transaction, H160, H256, U256},
    utils::keccak256,
};
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    stream::Stream,
    task::{Context, Poll},
    Future, FutureExt,
};
use revm::{db::DatabaseRef, AccountInfo, Bytecode, KECCAK_EMPTY};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::{
        mpsc::{channel as oneshot_channel, Sender as OneshotSender},
        Arc,
    },
};
use tracing::{error, trace, warn};

// Various future/request type aliases

type AccountFuture<Err> =
    Pin<Box<dyn Future<Output = (Result<(U256, U256, Bytes), Err>, Address)> + Send>>;
type StorageFuture<Err> = Pin<Box<dyn Future<Output = (Result<U256, Err>, Address, U256)> + Send>>;
type BlockHashFuture<Err> = Pin<Box<dyn Future<Output = (Result<H256, Err>, u64)> + Send>>;
type FullBlockFuture<Err> = Pin<
    Box<
        dyn Future<Output = (FullBlockSender, Result<Option<Block<Transaction>>, Err>, BlockId)>
            + Send,
    >,
>;
type TransactionFuture<Err> = Pin<
    Box<dyn Future<Output = (TransactionSender, Result<Option<Transaction>, Err>, H256)> + Send>,
>;

type AccountInfoSender = OneshotSender<DatabaseResult<AccountInfo>>;
type StorageSender = OneshotSender<DatabaseResult<U256>>;
type BlockHashSender = OneshotSender<DatabaseResult<H256>>;
type FullBlockSender = OneshotSender<DatabaseResult<Block<Transaction>>>;
type TransactionSender = OneshotSender<DatabaseResult<Transaction>>;

/// Request variants that are executed by the provider
enum ProviderRequest<Err> {
    Account(AccountFuture<Err>),
    Storage(StorageFuture<Err>),
    BlockHash(BlockHashFuture<Err>),
    FullBlock(FullBlockFuture<Err>),
    Transaction(TransactionFuture<Err>),
}

/// The Request type the Backend listens for
#[derive(Debug)]
enum BackendRequest {
    /// Fetch the account info
    Basic(Address, AccountInfoSender),
    /// Fetch a storage slot
    Storage(Address, U256, StorageSender),
    /// Fetch a block hash
    BlockHash(u64, BlockHashSender),
    /// Fetch an entire block with transactions
    FullBlock(BlockId, FullBlockSender),
    /// Fetch a transaction
    Transaction(H256, TransactionSender),
    /// Sets the pinned block to fetch data from
    SetPinnedBlock(BlockId),
}

/// Handles an internal provider and listens for requests.
///
/// This handler will remain active as long as it is reachable (request channel still open) and
/// requests are in progress.
#[must_use = "BackendHandler does nothing unless polled."]
pub struct BackendHandler<M: Middleware> {
    provider: M,
    /// Stores all the data.
    db: BlockchainDb,
    /// Requests currently in progress
    pending_requests: Vec<ProviderRequest<M::Error>>,
    /// Listeners that wait for a `get_account` related response
    account_requests: HashMap<Address, Vec<AccountInfoSender>>,
    /// Listeners that wait for a `get_storage_at` response
    storage_requests: HashMap<(Address, U256), Vec<StorageSender>>,
    /// Listeners that wait for a `get_block` response
    block_requests: HashMap<u64, Vec<BlockHashSender>>,
    /// Incoming commands.
    incoming: Receiver<BackendRequest>,
    /// unprocessed queued requests
    queued_requests: VecDeque<BackendRequest>,
    /// The block to fetch data from.
    // This is an `Option` so that we can have less code churn in the functions below
    block_id: Option<BlockId>,
}

impl<M> BackendHandler<M>
where
    M: Middleware + Clone + 'static,
{
    fn new(
        provider: M,
        db: BlockchainDb,
        rx: Receiver<BackendRequest>,
        block_id: Option<BlockId>,
    ) -> Self {
        Self {
            provider,
            db,
            pending_requests: Default::default(),
            account_requests: Default::default(),
            storage_requests: Default::default(),
            block_requests: Default::default(),
            queued_requests: Default::default(),
            incoming: rx,
            block_id,
        }
    }

    /// handle the request in queue in the future.
    ///
    /// We always check:
    ///  1. if the requested value is already stored in the cache, then answer the sender
    ///  2. otherwise, fetch it via the provider but check if a request for that value is already in
    /// progress (e.g. another Sender just requested the same account)
    fn on_request(&mut self, req: BackendRequest) {
        match req {
            BackendRequest::Basic(addr, sender) => {
                trace!(target: "backendhandler", "received request basic address={:?}", addr);
                let acc = self.db.accounts().read().get(&addr).cloned();
                if let Some(basic) = acc {
                    let _ = sender.send(Ok(basic));
                } else {
                    self.request_account(addr, sender);
                }
            }
            BackendRequest::BlockHash(number, sender) => {
                let hash = self.db.block_hashes().read().get(&U256::from(number)).cloned();
                if let Some(hash) = hash {
                    let _ = sender.send(Ok(hash));
                } else {
                    self.request_hash(number, sender);
                }
            }
            BackendRequest::FullBlock(number, sender) => {
                self.request_full_block(number, sender);
            }
            BackendRequest::Transaction(tx, sender) => {
                self.request_transaction(tx, sender);
            }
            BackendRequest::Storage(addr, idx, sender) => {
                // account is already stored in the cache
                let value =
                    self.db.storage().read().get(&addr).and_then(|acc| acc.get(&idx).copied());
                if let Some(value) = value {
                    let _ = sender.send(Ok(value));
                } else {
                    let addresses: HashSet<H160> = HashSet::from([
                        "0xc36442b4a4522e871399cd717abdd847ab11fe88".parse().unwrap(),
                        "0x8f16A8E864162ec84a2906646E08a561b5A7f72d".parse().unwrap(),
                        "0x50eaEDB835021E4A108B7290636d62E9765cc6d7".parse().unwrap(),
                        "0xB2de34D087B01803fBC1e494C0b1436B30679EC9".parse().unwrap(),
                        "0x45dDa9cb7c25131DF268515131f647d726f50608".parse().unwrap(),
                        "0x392362eecf08CaF40E642C3d52D2c77e95AD4a89".parse().unwrap(),
                        "0xAb273Ee58B549EdA50381a6a96076c3397e506dC".parse().unwrap(),
                        "0x309C06063D11f7f208192537308ac0708fE2F71a".parse().unwrap(),
                        "0xD6362B92799bcb8a99DE903C682D1bB72EBACfbf".parse().unwrap(),
                        "0x5f751C242D0c7cf997Cf38b355C83F6F75F7D60e".parse().unwrap(),
                        "0x0312692E9cADD3dDaaCE2E112A4e36397bd2f18a".parse().unwrap(),
                        "0x07A0e5CC33F3f28Cf655D7a76334D0b1BAB5b704".parse().unwrap(),
                        "0x4CcD010148379ea531D6C587CfDd60180196F9b1".parse().unwrap(),
                        "0xbD105Ef875f7265100fE8c3337498F9fB66061D7".parse().unwrap(),
                        "0x5F3799E894d7579BC69624B24F8f8B90E09c567E".parse().unwrap(),
                        "0x2C544cAE7B8c4bD4EaE77d2Efce6Cb5D5495D9f5".parse().unwrap(),
                        "0xa9077cDB3D13F45B8B9d87C43E11bCe0e73d8631".parse().unwrap(),
                        "0xEa8a6F1e5D53C89A00718b7ecDCeF70592a4b3F2".parse().unwrap(),
                        "0x9789dbA3492FE8b3D39bB59Ad32507DF21a96c7c".parse().unwrap(),
                        "0xED26b64F351A04Acbf16AA3782969e4a53B12c54".parse().unwrap(),
                        "0x2aCeda63B5e958c45bd27d916ba701BC1DC08F7a".parse().unwrap(),
                        "0x167384319B41F7094e62f7506409Eb38079AbfF8".parse().unwrap(),
                        "0x7CF58c7122FbF6C4361A20aaB0f6e2dD05455666".parse().unwrap(),
                        "0x36af44681EBE80Cdd1b285703472C9d7A6fb6D0D".parse().unwrap(),
                        "0x642F28a89fa9d0Fa30e664F71804Bfdd7341D21f".parse().unwrap(),
                        "0xc3D2E715ebF05c7dfAdf70A2e9C87AeA973680e1".parse().unwrap(),
                        "0x934fC18B73E51F247548FfE2d86108502B18037a".parse().unwrap(),
                        "0xBbefB500829c4e13cF5Bbd6549f6ddDd8A38fE12".parse().unwrap(),
                        "0x4fe1269a585B141F11C3E144158f9f8823c7C0e7".parse().unwrap(),
                        "0x3e31AB7f37c048FC6574189135D108df80F0ea26".parse().unwrap(),
                        "0x4D05f2A005e6F36633778416764E82d1D12E7fbb".parse().unwrap(),
                        "0x64b6cd3a85b5902A267e953C6ac1DBa376f0a999".parse().unwrap(),
                        "0x98b9162161164De1ED182A0dFA08f5fBf0F733CA".parse().unwrap(),
                        "0x9B08288C3Be4F62bbf8d1C20Ac9C5e6f9467d8B7".parse().unwrap(),
                        "0xFE530931dA161232Ec76A7c3bEA7D36cF3811A0d".parse().unwrap(),
                        "0x141688f5EAB27CBa15b5588f011bdEfc25F9Eae6".parse().unwrap(),
                        "0xb12b9945BFc5cf4b47e033360B0CD0de9494ea11".parse().unwrap(),
                        "0x60ed64A53c15460a7b6409779cd89c3A74876f7d".parse().unwrap(),
                        "0x145c42b9002c84e67bE444C0C3525Fd13dfeB888".parse().unwrap(),
                        "0x75Cfe424F62135FBdF8651B7EbCbfb99FC982eF8".parse().unwrap(),
                        "0x6baD0f9a89Ca403bb91d253D385CeC1A2b6eca97".parse().unwrap(),
                        "0xe26cfe704029EaCA5Cb37759057f41cA58F63b68".parse().unwrap(),
                        "0x5778c87A9934E0608860AC339A43F4B184ceA025".parse().unwrap(),
                        "0xbA1c003714A7a1C49F34839D6a3eA1e8088A0552".parse().unwrap(),
                        "0x12CfbfFF0Ea06A04DfA50d8597143313C3Dd75d4".parse().unwrap(),
                        "0xdf1CD8D8C333f02db3DE558Ee057bA20E0726E80".parse().unwrap(),
                        "0xB45D091E37878A90b53af2A33FaF9997983Ef2fB".parse().unwrap(),
                        "0xc661Ef6A4522894037347690f83Ee3A631452Bfa".parse().unwrap(),
                        "0x5796062B5a8D207698373be746C974623CE521C0".parse().unwrap(),
                        "0xFC99D1c02D27DE07DfE0dCd878CDe86ee59c5f6B".parse().unwrap(),
                        "0x239F4CbeEAfFC4d6A94d0c1363D5f228A9c002Bc".parse().unwrap(),
                        "0x59db5eA66958b19641b6891Fc373B44b567ea15C".parse().unwrap(),
                        "0x81a15f595F0563cFf256F6D5b82525EF355bc3Ab".parse().unwrap(),
                        "0x09d17750523d90989f66C4B4bB087d8AB8550C34".parse().unwrap(),
                        "0xA76A22c8da733B38952bfF95360fe50319D28A3a".parse().unwrap(),
                        "0xc897b8fa8a26b1014DA621800BDca6B2A935dc6d".parse().unwrap(),
                        "0xb23988572C1E4e9134d91662E7c64CE68bAb63b8".parse().unwrap(),
                        "0xf7024fF3Cd59e1942B6DE1770b296162fe97E6A1".parse().unwrap(),
                        "0xd738aFf65485Ec1A3726E28Bf8A9D0cF47849842".parse().unwrap(),
                        "0x15729E1eAB9C8C4E4cBDe4A510552d221A298d9d".parse().unwrap(),
                        "0xA374094527e1673A86dE625aa59517c5dE346d32".parse().unwrap(),
                        "0x20FA36FD5F7D6f5aE8E41F8dd41DdB01b40b1E6a".parse().unwrap(),
                        "0xc63123aec88F6965D2792E96f9E8a3324dbBC6B0".parse().unwrap(),
                        "0xCC1176cA8faa3b9290262A61bEF409989A2BA406".parse().unwrap(),
                        "0x6F4221A592771fDE742157A413ed0127294F507e".parse().unwrap(),
                        "0x83049A82b1F5DcBDf915fD5524DB64b25c2e6181".parse().unwrap(),
                        "0x7FB09Ae882e5A50198322a27858eEFAf14F88951".parse().unwrap(),
                        "0x75B3F71E2c5f0aa465f29eEafc6A59Ebfa713dd3".parse().unwrap(),
                        "0xB85CD18FF4dd8a14092A64110Ff47c991A404bD5".parse().unwrap(),
                        "0xB6F484270A5bEE5c6f08939E78c09eBc13396B4F".parse().unwrap(),
                        "0xF76BF9193E512B7F23b7B8C59534363216F19569".parse().unwrap(),
                        "0x027be85FF9481399eC5c6b854c77D5E5ca98905b".parse().unwrap(),
                        "0xA6bF2dfD2063adD67e45Ee632751484C601853B9".parse().unwrap(),
                        "0x4281b0cf4D7bA729B4745d1480D46fF2077E1400".parse().unwrap(),
                        "0xfBD8675514a567d9B8B02eCc8dc7B7184c963dFD".parse().unwrap(),
                        "0xbE16B0a1CB25FDe3674729D25331582f462aAA50".parse().unwrap(),
                        "0x319BA2683bB64D294A3Fc47D8aA12C11804Ef6C8".parse().unwrap(),
                        "0xB34e4e5FA756D9e4f58A3F2AD7C8060608B60D16".parse().unwrap(),
                        "0xe11A3961d16120806d2C5736C27bE1cEbEa16A49".parse().unwrap(),
                        "0x26770cC2c612e5C97eb70A6531Fa9d5098ef74ab".parse().unwrap(),
                        "0x046495d9B94749da1A904A82752c482CD5bB5477".parse().unwrap(),
                        "0xe4F6d42699c05622EeD7f4D77C2a12eEdafAD288".parse().unwrap(),
                        "0xB2bBFF3001530ad4A27d17b68925B07004EF9FEC".parse().unwrap(),
                        "0xC4E93160B6AfD891c4e9446A5880E8Da95B790f1".parse().unwrap(),
                        "0xAc101643d3529b1FbD82ee202a7b5d5BfC5cc503".parse().unwrap(),
                        "0x1c1b5fe173A8585c054Ddb8BF8be19EF6F78EBf7".parse().unwrap(),
                        "0xA4D054067dc3077ad82b9b56e9cCf6Cf462cf153".parse().unwrap(),
                        "0xfaE91e08188332ED6Ea09b314BBE3FC6cD1432C1".parse().unwrap(),
                        "0x0c61D1E9bBEC03cA9d1cA2Db0C94fa08AC91Ad5a".parse().unwrap(),
                        "0x7Ee82fB5c623D088b08FF73302ea3807651604bA".parse().unwrap(),
                        "0x5B45481eDb565bF69617E1055Ed70003a568ad9c".parse().unwrap(),
                        "0x2d8e5F3bbb21e1A3B52dF89007B0CC8b09bA8523".parse().unwrap(),
                        "0x97b226fbA30EEc8D8239e20cd73da1c6584D4da2".parse().unwrap(),
                        "0xD3AFe4439Af46B1686d366EC0C94EfFBA732560f".parse().unwrap(),
                        "0x33016DF701b323c33cc027146c6a9e0997B2a923".parse().unwrap(),
                        "0xcb06ACb8E98344dB73503797df9974c01bEDc776".parse().unwrap(),
                        "0x62b5305ECbA9BeBBa2A925ef7711566FD1a2080d".parse().unwrap(),
                        "0x293E896eEaB9e7805aec3b51Eb7A3Cf2970208Bb".parse().unwrap(),
                        "0xd0beb9570bC3d2AF7FA461dFD3c75cC3a34f95e2".parse().unwrap(),
                        "0x847b64f9d3A95e977D157866447a5C0A5dFa0Ee5".parse().unwrap(),
                        "0xaAA7eD7cAc27e252031fC6db0B129Cdc65381369".parse().unwrap(),
                        "0xa6Ae01Ef41559107B46FC40e9b568D5D915298aA".parse().unwrap(),
                        "0x7D708892d1C99D546274f1171115bA1274468660".parse().unwrap(),
                        "0xf50c9E296e510711e8864CC71Bd2a5a7dC482cD5".parse().unwrap(),
                        "0x93dc552DD75a19C70C2053F00d081076F3e57dBF".parse().unwrap(),
                        "0x957b20FAED9aA6275eA3F7F1A0B7a87B4e139089".parse().unwrap(),
                        "0x806247E62f3b452455E19c1fb2Ca39194FC064a1".parse().unwrap(),
                        "0x9DF2F94A6E9eccf61795215B4144D5CaCE28Fa67".parse().unwrap(),
                        "0x5B0dA3A7962F783fa3f2496eec4f9B8BA27B83F6".parse().unwrap(),
                        "0xC4aF86cC7525a6e051705c65f03E97753439A634".parse().unwrap(),
                        "0xE186B2371E03f4baDeEc4E7E513758E7aaF590CD".parse().unwrap(),
                        "0xe2a3a86a7A7D222a6d8Da0E7B17847DE996792CD".parse().unwrap(),
                        "0x7C86c986808E01134e53B9e84701C41025bCe75F".parse().unwrap(),
                        "0xa236278beC0e0677A48527340cfB567b4E6e9AdC".parse().unwrap(),
                        "0xfe53614ee298FC419c72911DcEb7E203b699DAf8".parse().unwrap(),
                        "0x153D9ecFbe79FFecdB2aACfab5Abc4756F5f0CF0".parse().unwrap(),
                        "0x00B3c6507C114a6E57BCd10125D2E17c162bf123".parse().unwrap(),
                        "0xC212C034b06Ab5f1cE0Bc3e10164B22eA6dF443a".parse().unwrap(),
                        "0x65fb260aF0e8A2E518445F0861aCEf1538125428".parse().unwrap(),
                        "0xB5Fa364Db1e73D178BCa190A17D7D8bD335720f3".parse().unwrap(),
                        "0xDaC8A8E6DBf8c690ec6815e0fF03491B2770255D".parse().unwrap(),
                        "0x94Ab9E4553fFb839431E37CC79ba8905f45BfBeA".parse().unwrap(),
                        "0x5645dCB64c059aa11212707fbf4E7F984440a8Cf".parse().unwrap(),
                        "0x144b45fcB022201A312c7A415EB7B660906FdeC7".parse().unwrap(),
                        "0xa41Ff166d20b977c14FB37fbAE77cDF0720Bd0d8".parse().unwrap(),
                        "0xA3bB904FaB29A7AF3d11d7C4DE39fcb2f8A61159".parse().unwrap(),
                        "0xdB1BB6d37Db4DB7AF30E6788AAc83d727784E84A".parse().unwrap(),
                        "0x27641046dd60F0A8277DA08712c9b21dBbcE93Ae".parse().unwrap(),
                        "0x84AA7c85D25c3e1dDC80217af094152D1CeD0fd4".parse().unwrap(),
                        "0xc69F76Ff06C3f49EB7336d8e7045497Ac2E8aD9c".parse().unwrap(),
                        "0xb5463951C4e2aa3Da1Eb709da4356ad4984a8337".parse().unwrap(),
                        "0x944A52692ea6cb6C59d24f6658609cE72bB94FA0".parse().unwrap(),
                        "0xd20f057B05F1D62c1fe306f6EE77ab4c8fD7e2FB".parse().unwrap(),
                        "0x20fB28b6209eD80eeE40894BAA9a199C07B03e55".parse().unwrap(),
                        "0x0fC0C3Ca9dD502F26f148dDe38a58BA2b2d16C6b".parse().unwrap(),
                        "0xA1AFF82C6f22fc089febF116b5034f64F550ec09".parse().unwrap(),
                        "0x59db4C2feBCEA4Fdc715dB6283966f10e0c5829E".parse().unwrap(),
                        "0xc29b1f8432fF91975e485a8C50166A4461a63CB6".parse().unwrap(),
                        "0x017F2c4f3986316c6Ab53e854fe87C03F3a9c845".parse().unwrap(),
                        "0x34257a13dD67F509930D6Ea6266d308DAf8FB4Aa".parse().unwrap(),
                        "0xC9D0CAe8343a2231b1647Ab00e639eAbdC766147".parse().unwrap(),
                        "0x8e9723391FF520fC79A019D9A2022bb92c1B44fB".parse().unwrap(),
                        "0xc50bFaD8088e55310F83c62Ff6b3F3BaFcE3DA35".parse().unwrap(),
                        "0x34E51Bb3b34772D3605fD67A05fBd44803A23577".parse().unwrap(),
                        "0x3Fa147D6309abeb5C1316f7d8a7d8bD023e0cd80".parse().unwrap(),
                        "0xB114b5d471A786e9b3AF72F929C2d103087e75fF".parse().unwrap(),
                        "0x5159E9D1D0e6932E834EE7909d177397514E25E7".parse().unwrap(),
                        "0x7de263D0Ad6e5D208844E65118c3a02A9A5D56B6".parse().unwrap(),
                        "0xC6CaFd8Db3114E8980194120a3c6717cB94C70fC".parse().unwrap(),
                        "0xf32E4c97ebA44D89A0B16f7a6285681e989B86dC".parse().unwrap(),
                        "0x5884DDB0bb109C02150242EDf00d0737D78eD61D".parse().unwrap(),
                        "0x6c8408F735C2f73b8f9271F663B1540bb8C2ACcE".parse().unwrap(),
                        "0x254aa3A898071D6A2dA0DB11dA73b02B4646078F".parse().unwrap(),
                        "0x78E636989978fb2A5D9cf0B9e8E0B008890A8E1D".parse().unwrap(),
                    ]);
                    if addresses.contains(&addr) && idx > U256::from(100_000_000) {
                        let _ = sender.send(Ok(U256::zero()));
                    } else {
                        // account present but not storage -> fetch storage
                        self.request_account_storage(addr, idx, sender);
                    }
                }
            }
            BackendRequest::SetPinnedBlock(block_id) => {
                self.block_id = Some(block_id);
            }
        }
    }

    /// process a request for account's storage
    fn request_account_storage(&mut self, address: Address, idx: U256, listener: StorageSender) {
        match self.storage_requests.entry((address, idx)) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(listener);
            }
            Entry::Vacant(entry) => {
                trace!(target: "backendhandler", "preparing storage request, address={:?}, idx={}", address, idx);
                entry.insert(vec![listener]);
                let provider = self.provider.clone();
                let block_id = self.block_id;
                let fut = Box::pin(async move {
                    // serialize & deserialize back to U256
                    let idx_req = H256::from_uint(&idx);
                    let storage = provider.get_storage_at(address, idx_req, block_id).await;
                    let storage = storage.map(|storage| storage.into_uint());
                    (storage, address, idx)
                });
                self.pending_requests.push(ProviderRequest::Storage(fut));
            }
        }
    }

    /// returns the future that fetches the account data
    fn get_account_req(&self, address: Address) -> ProviderRequest<M::Error> {
        trace!(target: "backendhandler", "preparing account request, address={:?}", address);
        let provider = self.provider.clone();
        let block_id = self.block_id;
        let fut = Box::pin(async move {
            let balance = provider.get_balance(address, block_id);
            let nonce = provider.get_transaction_count(address, block_id);
            let code = provider.get_code(address, block_id);
            let resp = tokio::try_join!(balance, nonce, code);
            (resp, address)
        });
        ProviderRequest::Account(fut)
    }

    /// process a request for an account
    fn request_account(&mut self, address: Address, listener: AccountInfoSender) {
        match self.account_requests.entry(address) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(listener);
            }
            Entry::Vacant(entry) => {
                entry.insert(vec![listener]);
                self.pending_requests.push(self.get_account_req(address));
            }
        }
    }

    /// process a request for an entire block
    fn request_full_block(&mut self, number: BlockId, sender: FullBlockSender) {
        let provider = self.provider.clone();
        let fut = Box::pin(async move {
            let block = provider.get_block_with_txs(number).await;
            (sender, block, number)
        });

        self.pending_requests.push(ProviderRequest::FullBlock(fut));
    }

    /// process a request for a transactions
    fn request_transaction(&mut self, tx: H256, sender: TransactionSender) {
        let provider = self.provider.clone();
        let fut = Box::pin(async move {
            let block = provider.get_transaction(tx).await;
            (sender, block, tx)
        });

        self.pending_requests.push(ProviderRequest::Transaction(fut));
    }

    /// process a request for a block hash
    fn request_hash(&mut self, number: u64, listener: BlockHashSender) {
        match self.block_requests.entry(number) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(listener);
            }
            Entry::Vacant(entry) => {
                trace!(target: "backendhandler", "preparing block hash request, number={}", number);
                entry.insert(vec![listener]);
                let provider = self.provider.clone();
                let fut = Box::pin(async move {
                    let block = provider.get_block(number).await;

                    let block_hash = match block {
                        Ok(Some(block)) => Ok(block
                            .hash
                            .expect("empty block hash on mined block, this should never happen")),
                        Ok(None) => {
                            warn!(target: "backendhandler", ?number, "block not found");
                            // if no block was returned then the block does not exist, in which case
                            // we return empty hash
                            Ok(KECCAK_EMPTY)
                        }
                        Err(err) => {
                            error!(target: "backendhandler", ?err, ?number, "failed to get block");
                            Err(err)
                        }
                    };
                    (block_hash, number)
                });
                self.pending_requests.push(ProviderRequest::BlockHash(fut));
            }
        }
    }
}

impl<M> Future for BackendHandler<M>
where
    M: Middleware + Clone + Unpin + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let pin = self.get_mut();
        loop {
            // Drain queued requests first.
            while let Some(req) = pin.queued_requests.pop_front() {
                pin.on_request(req)
            }

            // receive new requests to delegate to the underlying provider
            loop {
                match Pin::new(&mut pin.incoming).poll_next(cx) {
                    Poll::Ready(Some(req)) => {
                        pin.queued_requests.push_back(req);
                    }
                    Poll::Ready(None) => {
                        trace!(target: "backendhandler", "last sender dropped, ready to drop (&flush cache)");
                        return Poll::Ready(())
                    }
                    Poll::Pending => break,
                }
            }

            // poll all requests in progress
            for n in (0..pin.pending_requests.len()).rev() {
                let mut request = pin.pending_requests.swap_remove(n);
                match &mut request {
                    ProviderRequest::Account(fut) => {
                        if let Poll::Ready((resp, addr)) = fut.poll_unpin(cx) {
                            // get the response
                            let (balance, nonce, code) = match resp {
                                Ok(res) => res,
                                Err(err) => {
                                    let err = Arc::new(eyre::Error::new(err));
                                    if let Some(listeners) = pin.account_requests.remove(&addr) {
                                        listeners.into_iter().for_each(|l| {
                                            let _ = l.send(Err(DatabaseError::GetAccount(
                                                addr,
                                                Arc::clone(&err),
                                            )));
                                        })
                                    }
                                    continue
                                }
                            };

                            // convert it to revm-style types
                            let (code, code_hash) = if !code.0.is_empty() {
                                (Some(code.0.clone()), keccak256(&code).into())
                            } else {
                                (Some(bytes::Bytes::default()), KECCAK_EMPTY)
                            };

                            // update the cache
                            let acc = AccountInfo {
                                nonce: nonce.as_u64(),
                                balance,
                                code: code.map(|bytes| Bytecode::new_raw(bytes).to_checked()),
                                code_hash,
                            };
                            pin.db.accounts().write().insert(addr, acc.clone());

                            // notify all listeners
                            if let Some(listeners) = pin.account_requests.remove(&addr) {
                                listeners.into_iter().for_each(|l| {
                                    let _ = l.send(Ok(acc.clone()));
                                })
                            }
                            continue
                        }
                    }
                    ProviderRequest::Storage(fut) => {
                        if let Poll::Ready((resp, addr, idx)) = fut.poll_unpin(cx) {
                            let value = match resp {
                                Ok(value) => value,
                                Err(err) => {
                                    // notify all listeners
                                    let err = Arc::new(eyre::Error::new(err));
                                    if let Some(listeners) =
                                        pin.storage_requests.remove(&(addr, idx))
                                    {
                                        listeners.into_iter().for_each(|l| {
                                            let _ = l.send(Err(DatabaseError::GetStorage(
                                                addr,
                                                idx,
                                                Arc::clone(&err),
                                            )));
                                        })
                                    }
                                    continue
                                }
                            };

                            // update the cache
                            pin.db.storage().write().entry(addr).or_default().insert(idx, value);

                            // notify all listeners
                            if let Some(listeners) = pin.storage_requests.remove(&(addr, idx)) {
                                listeners.into_iter().for_each(|l| {
                                    let _ = l.send(Ok(value));
                                })
                            }
                            continue
                        }
                    }
                    ProviderRequest::BlockHash(fut) => {
                        if let Poll::Ready((block_hash, number)) = fut.poll_unpin(cx) {
                            let value = match block_hash {
                                Ok(value) => value,
                                Err(err) => {
                                    let err = Arc::new(eyre::Error::new(err));
                                    // notify all listeners
                                    if let Some(listeners) = pin.block_requests.remove(&number) {
                                        listeners.into_iter().for_each(|l| {
                                            let _ = l.send(Err(DatabaseError::GetBlockHash(
                                                number,
                                                Arc::clone(&err),
                                            )));
                                        })
                                    }
                                    continue
                                }
                            };

                            // update the cache
                            pin.db.block_hashes().write().insert(number.into(), value);

                            // notify all listeners
                            if let Some(listeners) = pin.block_requests.remove(&number) {
                                listeners.into_iter().for_each(|l| {
                                    let _ = l.send(Ok(value));
                                })
                            }
                            continue
                        }
                    }
                    ProviderRequest::FullBlock(fut) => {
                        if let Poll::Ready((sender, resp, number)) = fut.poll_unpin(cx) {
                            let msg = match resp {
                                Ok(Some(block)) => Ok(block),
                                Ok(None) => Err(DatabaseError::BlockNotFound(number)),
                                Err(err) => {
                                    let err = Arc::new(eyre::Error::new(err));
                                    Err(DatabaseError::GetFullBlock(number, err))
                                }
                            };
                            let _ = sender.send(msg);
                            continue
                        }
                    }
                    ProviderRequest::Transaction(fut) => {
                        if let Poll::Ready((sender, tx, tx_hash)) = fut.poll_unpin(cx) {
                            let msg = match tx {
                                Ok(Some(tx)) => Ok(tx),
                                Ok(None) => Err(DatabaseError::TransactionNotFound(tx_hash)),
                                Err(err) => {
                                    let err = Arc::new(eyre::Error::new(err));
                                    Err(DatabaseError::GetTransaction(tx_hash, err))
                                }
                            };
                            let _ = sender.send(msg);
                            continue
                        }
                    }
                }
                // not ready, insert and poll again
                pin.pending_requests.push(request);
            }

            // If no new requests have been queued, break to
            // be polled again later.
            if pin.queued_requests.is_empty() {
                return Poll::Pending
            }
        }
    }
}

/// A cloneable backend type that shares access to the backend data with all its clones.
///
/// This backend type is connected to the `BackendHandler` via a mpsc channel. The `BackendHandler`
/// is spawned on a tokio task and listens for incoming commands on the receiver half of the
/// channel. A `SharedBackend` holds a sender for that channel, which is `Clone`, so there can be
/// multiple `SharedBackend`s communicating with the same `BackendHandler`, hence this `Backend`
/// type is thread safe.
///
/// All `Backend` trait functions are delegated as a `BackendRequest` via the channel to the
/// `BackendHandler`. All `BackendRequest` variants include a sender half of an additional channel
/// that is used by the `BackendHandler` to send the result of an executed `BackendRequest` back to
/// `SharedBackend`.
///
/// The `BackendHandler` holds an ethers `Provider` to look up missing accounts or storage slots
/// from remote (e.g. infura). It detects duplicate requests from multiple `SharedBackend`s and
/// bundles them together, so that always only one provider request is executed. For example, there
/// are two `SharedBackend`s, `A` and `B`, both request the basic account info of account
/// `0xasd9sa7d...` at the same time. After the `BackendHandler` receives the request from `A`, it
/// sends a new provider request to the provider's endpoint, then it reads the identical request
/// from `B` and simply adds it as an additional listener for the request already in progress,
/// instead of sending another one. So that after the provider returns the response all listeners
/// (`A` and `B`) get notified.
// **Note**: the implementation makes use of [tokio::task::block_in_place()] when interacting with
// the underlying [BackendHandler] which runs on a separate spawned tokio task.
// [tokio::task::block_in_place()]
// > Runs the provided blocking function on the current thread without blocking the executor.
// This prevents issues (hangs) we ran into were the [SharedBackend] itself is called from a spawned
// task.
#[derive(Debug, Clone)]
pub struct SharedBackend {
    /// channel used for sending commands related to database operations
    backend: Sender<BackendRequest>,
    /// Ensures that the underlying cache gets flushed once the last `SharedBackend` is dropped.
    ///
    /// There is only one instance of the type, so as soon as the last `SharedBackend` is deleted,
    /// `FlushJsonBlockCacheDB` is also deleted and the cache is flushed.
    cache: Arc<FlushJsonBlockCacheDB>,
}

impl SharedBackend {
    /// _Spawns_ a new `BackendHandler` on a `tokio::task` that listens for requests from any
    /// `SharedBackend`. Missing values get inserted in the `db`.
    ///
    /// The spawned `BackendHandler` finishes once the last `SharedBackend` connected to it is
    /// dropped.
    ///
    /// NOTE: this should be called with `Arc<Provider>`
    pub async fn spawn_backend<M>(provider: M, db: BlockchainDb, pin_block: Option<BlockId>) -> Self
    where
        M: Middleware + Unpin + 'static + Clone,
    {
        let (shared, handler) = Self::new(provider, db, pin_block);
        // spawn the provider handler to a task
        trace!(target: "backendhandler", "spawning Backendhandler task");
        tokio::spawn(handler);
        shared
    }

    /// Same as `Self::spawn_backend` but spawns the `BackendHandler` on a separate `std::thread` in
    /// its own `tokio::Runtime`
    pub fn spawn_backend_thread<M>(
        provider: M,
        db: BlockchainDb,
        pin_block: Option<BlockId>,
    ) -> Self
    where
        M: Middleware + Unpin + 'static + Clone,
    {
        let (shared, handler) = Self::new(provider, db, pin_block);

        // spawn a light-weight thread with a thread-local async runtime just for
        // sending and receiving data from the remote client
        let _ = std::thread::Builder::new()
            .name("fork-backend-thread".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create fork-backend-thread tokio runtime");

                rt.block_on(async move { handler.await });
            })
            .expect("failed to spawn backendhandler thread");
        trace!(target: "backendhandler", "spawned Backendhandler thread");

        shared
    }

    /// Returns a new `SharedBackend` and the `BackendHandler`
    pub fn new<M>(
        provider: M,
        db: BlockchainDb,
        pin_block: Option<BlockId>,
    ) -> (Self, BackendHandler<M>)
    where
        M: Middleware + Unpin + 'static + Clone,
    {
        let (backend, backend_rx) = channel(1);
        let cache = Arc::new(FlushJsonBlockCacheDB(Arc::clone(db.cache())));
        let handler = BackendHandler::new(provider, db, backend_rx, pin_block);
        (Self { backend, cache }, handler)
    }

    /// Updates the pinned block to fetch data from
    pub fn set_pinned_block(&self, block: impl Into<BlockId>) -> eyre::Result<()> {
        let req = BackendRequest::SetPinnedBlock(block.into());
        self.backend.clone().try_send(req).map_err(|e| eyre::eyre!("{:?}", e))
    }

    /// Returns the full block for the given block identifier
    pub fn get_full_block(&self, block: impl Into<BlockId>) -> DatabaseResult<Block<Transaction>> {
        tokio::task::block_in_place(|| {
            let (sender, rx) = oneshot_channel();
            let req = BackendRequest::FullBlock(block.into(), sender);
            self.backend.clone().try_send(req)?;
            rx.recv()?
        })
    }

    /// Returns the transaction for the hash
    pub fn get_transaction(&self, tx: H256) -> DatabaseResult<Transaction> {
        tokio::task::block_in_place(|| {
            let (sender, rx) = oneshot_channel();
            let req = BackendRequest::Transaction(tx, sender);
            self.backend.clone().try_send(req)?;
            rx.recv()?
        })
    }

    fn do_get_basic(&self, address: Address) -> DatabaseResult<Option<AccountInfo>> {
        tokio::task::block_in_place(|| {
            let (sender, rx) = oneshot_channel();
            let req = BackendRequest::Basic(address, sender);
            self.backend.clone().try_send(req)?;
            rx.recv()?.map(Some)
        })
    }

    fn do_get_storage(&self, address: Address, index: U256) -> DatabaseResult<U256> {
        tokio::task::block_in_place(|| {
            let (sender, rx) = oneshot_channel();
            let req = BackendRequest::Storage(address, index, sender);
            self.backend.clone().try_send(req)?;
            rx.recv()?
        })
    }

    fn do_get_block_hash(&self, number: u64) -> DatabaseResult<H256> {
        tokio::task::block_in_place(|| {
            let (sender, rx) = oneshot_channel();
            let req = BackendRequest::BlockHash(number, sender);
            self.backend.clone().try_send(req)?;
            rx.recv()?
        })
    }

    /// Flushes the DB to disk if caching is enabled
    pub(crate) fn flush_cache(&self) {
        self.cache.0.flush();
    }
}

impl DatabaseRef for SharedBackend {
    type Error = DatabaseError;

    fn basic(&self, address: H160) -> Result<Option<AccountInfo>, Self::Error> {
        trace!( target: "sharedbackend", "request basic {:?}", address);
        self.do_get_basic(address).map_err(|err| {
            error!(target: "sharedbackend",  ?err, ?address,  "Failed to send/recv `basic`");
            err
        })
    }

    fn code_by_hash(&self, hash: H256) -> Result<Bytecode, Self::Error> {
        Err(DatabaseError::MissingCode(hash))
    }

    fn storage(&self, address: H160, index: U256) -> Result<U256, Self::Error> {
        trace!( target: "sharedbackend", "request storage {:?} at {:?}", address, index);
        self.do_get_storage(address, index).map_err(|err| {
            error!( target: "sharedbackend", ?err, ?address, ?index, "Failed to send/recv `storage`");
          err
        })
    }

    fn block_hash(&self, number: U256) -> Result<H256, Self::Error> {
        if number > U256::from(u64::MAX) {
            return Ok(KECCAK_EMPTY)
        }
        let number = number.as_u64();
        trace!( target: "sharedbackend", "request block hash for number {:?}", number);
        self.do_get_block_hash(number).map_err(|err| {
            error!(target: "sharedbackend",?err, ?number, "Failed to send/recv `block_hash`");
            err
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{
        fork::{BlockchainDbMeta, CreateFork, JsonBlockCacheDB},
        opts::EvmOpts,
        Backend,
    };
    use ethers::{
        solc::utils::RuntimeOrHandle,
        types::{Address, Chain},
    };
    use foundry_common::get_http_provider;
    use foundry_config::Config;
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc};
    const ENDPOINT: &str = "https://mainnet.infura.io/v3/c60b0bb42f8a4c6481ecd229eddaca27";

    #[test]
    fn shared_backend() {
        let provider = get_http_provider(ENDPOINT);
        let meta = BlockchainDbMeta {
            cfg_env: Default::default(),
            block_env: Default::default(),
            hosts: BTreeSet::from([ENDPOINT.to_string()]),
        };

        let db = BlockchainDb::new(meta, None);
        let runtime = RuntimeOrHandle::new();
        let backend =
            runtime.block_on(SharedBackend::spawn_backend(Arc::new(provider), db.clone(), None));

        // some rng contract from etherscan
        let address: Address = "63091244180ae240c87d1f528f5f269134cb07b3".parse().unwrap();

        let idx = U256::from(0u64);
        let value = backend.storage(address, idx).unwrap();
        let account = backend.basic(address).unwrap().unwrap();

        let mem_acc = db.accounts().read().get(&address).unwrap().clone();
        assert_eq!(account.balance, mem_acc.balance);
        assert_eq!(account.nonce, mem_acc.nonce);
        let slots = db.storage().read().get(&address).unwrap().clone();
        assert_eq!(slots.len(), 1);
        assert_eq!(slots.get(&idx).copied().unwrap(), value);

        let num = U256::from(10u64);
        let hash = backend.block_hash(num).unwrap();
        let mem_hash = *db.block_hashes().read().get(&num).unwrap();
        assert_eq!(hash, mem_hash);

        let max_slots = 5;
        let handle = std::thread::spawn(move || {
            for i in 1..max_slots {
                let idx = U256::from(i);
                let _ = backend.storage(address, idx);
            }
        });
        handle.join().unwrap();
        let slots = db.storage().read().get(&address).unwrap().clone();
        assert_eq!(slots.len() as u64, max_slots);
    }

    #[test]
    fn can_read_cache() {
        let cache_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test-data/storage.json");
        let json = JsonBlockCacheDB::load(cache_path).unwrap();
        assert!(!json.db().accounts.read().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn can_read_write_cache() {
        let provider = get_http_provider(ENDPOINT);

        let block_num = provider.get_block_number().await.unwrap().as_u64();

        let config = Config::figment();
        let mut evm_opts = config.extract::<EvmOpts>().unwrap();
        evm_opts.fork_block_number = Some(block_num);

        let env = evm_opts.fork_evm_env(ENDPOINT).await.unwrap();

        let fork = CreateFork {
            enable_caching: true,
            url: ENDPOINT.to_string(),
            env: env.clone(),
            evm_opts,
        };

        let backend = Backend::spawn(Some(fork));

        // some rng contract from etherscan
        let address: Address = "63091244180ae240c87d1f528f5f269134cb07b3".parse().unwrap();

        let idx = U256::from(0u64);
        let _value = backend.storage(address, idx);
        let _account = backend.basic(address);

        // fill some slots
        let num_slots = 10u64;
        for idx in 1..num_slots {
            let _ = backend.storage(address, idx.into());
        }
        drop(backend);

        let meta =
            BlockchainDbMeta { cfg_env: env.cfg, block_env: env.block, hosts: Default::default() };

        let db = BlockchainDb::new(
            meta,
            Some(Config::foundry_block_cache_dir(Chain::Mainnet, block_num).unwrap()),
        );
        assert!(db.accounts().read().contains_key(&address));
        assert!(db.storage().read().contains_key(&address));
        assert_eq!(db.storage().read().get(&address).unwrap().len(), num_slots as usize);
    }
}
