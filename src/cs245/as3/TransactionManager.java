package cs245.as3;

import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.driver.LogManagerImpl;
import cs245.as3.driver.StorageManagerImpl;
import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
//	//设计一个序列化格式<len,txID,type,key,value>
//	private static class Record{
//		long txID;
//		long key;
//		byte[] value;
//		int len;
//		byte type;//write=0,commit=1
//
//		public Record(byte type,long txID, long key, byte[] value) {
//			this.txID = txID;
//			this.key = key;
//			this.value = value;
//			this.type=type;
//			len=2 * Long.BYTES+Integer.BYTES + value.length+1;
//		}
//
//		public Record(int len,byte type,long txID, long key, byte[] value) {
//			this.txID = txID;
//			this.key = key;
//			this.value = value;
//			this.len=len;
//			this.type=type;
//		}
//
//		public Record(long txID, byte type) {
//			this.txID = txID;
//			this.type = type;
//			len=Long.BYTES+1+Integer.BYTES;
//		}
//
//		public byte[] serialize(){
//			//申请空间
//			ByteBuffer buffer = ByteBuffer.allocate(len);
//			//封装存放数据
//			if (len==Long.BYTES+1+Integer.BYTES){
//				buffer.putInt(len);
//				buffer.putLong(txID);
//				buffer.put(type);
//				return buffer.array();
//			}else {
//				buffer.putInt(len);
//				buffer.putLong(txID);
//				buffer.put(type);
//				buffer.putLong(key);
//				buffer.put(value);
//				//返回结果
//				return buffer.array();
//			}
//
//		}
//
//		/**
//		 * 对传入数组解析成Record对象
//		 * @param b 传入的byte数组
//		 * @return 返回对象Record
//		 */
//		static Record deserialize(byte[] b){
//			ByteBuffer bb = ByteBuffer.wrap(b);
//			if (b.length==Long.BYTES+1+Integer.BYTES){
//				int len=bb.getInt();
//				long txID = bb.getLong();
//				byte type=bb.get();
//				return new Record(txID,type);
//			}else {
//				int len=bb.getInt();
//				long txID = bb.getLong();
//				byte type=bb.get();
//				long key = bb.getLong();
//				byte[] value = new byte[b.length - 2 * Long.BYTES-Integer.BYTES-1];
//				bb.get(value);
//				return new Record(len,type,txID,key,value);
//			}
//
//		}
//	}
	/**
	  * Holds the latest value for each key.
	 * 保存每个键的最新值。
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	 * 保存某事物的写操作，直到它被提交
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private LogManager lm;
	private StorageManager sm;
	//	记录当前的最大事务id，以确保下个start事务的合法性
	private long MaxTxID;
	// 	在writePersisted方法中使用
	private HashSet<Integer> OffsetSet;
	// 	存放已经进行持久化的日志record在lm中的偏移量
	private PriorityQueue<Integer> persisted;
	// 	key-txID,value-record日志集合
	private Map<Long,ArrayList<Record>> RecordMap;

	public TransactionManager() {
		writesets = new HashMap<>();
		//see initAndRecover
		latestValues = null;
		OffsetSet=new HashSet<>();
		persisted=new PriorityQueue<>();
		RecordMap=new HashMap<>();
		MaxTxID=-1;
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 * 准备事务管理器来服务操作。
	 * *此时你应该检测StorageManager是否不一致并恢复它。
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		//初始化
		//先对已persisted的key值进行读取
		latestValues = sm.readStoredTable();
		this.sm=sm;
		this.lm=lm;
		//数据库的恢复

		//存储自截断点以后的所有日志
		ArrayList<Record> Records = new ArrayList<>();
		//每条日志的偏移量(也就是日志尾巴的在lm的偏移量)
		ArrayList<Integer> tags = new ArrayList<>();
		//存放已提交事务事务id-txID
		Set<Long> txCommit = new HashSet<>();

		int logOffset=lm.getLogTruncationOffset();

		//从截断点读取日志record
		while (logOffset < lm.getLogEndOffset()) {

			//解析前4位获取此record日志的长度再进行日志的整条读取。
			byte[] bytes = lm.readLogRecord(logOffset, Integer.BYTES);
			ByteBuffer bb = ByteBuffer.wrap(bytes);
			//获取该条日志的偏移量
			int len=bb.getInt();
			byte[] R_bytes = lm.readLogRecord(logOffset, len);
			//得到该条日志记录
			Record record = Record.deserialize(R_bytes);

			Records.add(record);
			logOffset+=record.len;
			tags.add(logOffset);

			//记录已提交事务，为后续恢复做准备
			if (record.type==1){
				txCommit.add(record.txID);
			}
		}

		//遍历日志，执行txCommit里的事务
		for (int i = 0; i < Records.size(); i++) {
			Record record = Records.get(i);
			if (txCommit.contains(record.txID)&&record.type==0){
				Integer tag = tags.get(i);
				latestValues.put(record.key, new TaggedValue(tag,record.value));
				sm.queueWrite(record.key, tag,record.value);
				persisted.add(tag);
			}
		}
//		while (logOffset<lm.getLogEndOffset()){
//			byte[] bytes = lm.readLogRecord(logOffset, Integer.BYTES);
//			ByteBuffer bb = ByteBuffer.wrap(bytes);
//			int len=bb.getInt();
//			//知道该条日志的偏移量
//			byte[] R_bytes = lm.readLogRecord(logOffset, len);
//			//得到该条日志记录
//			Record record = Record.deserialize(R_bytes);
//			//写入StorageManager
//			sm.queueWrite(record.key, logOffset+len,record.value);
//			persisted.add(logOffset+len);
//			//更新latestValues
//			latestValues.put(record.key, new TaggedValue(logOffset+len, record.value));
//			logOffset+=len;
//			MaxTxID=record.txID;
//		}

	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 * 为开启一个新的事务，接口需要确保分配的txID单调递增（即使在系统crash后，仍需满足该特性）。
	 */

	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
		// 检查上一条事务id，该事务id是否大于上一条事务id
		if (txID>MaxTxID){
			MaxTxID=txID;
		}else try {
			throw new Exception("txid is not allow");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 * 返回任意事务最近提交的key的value。
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key.
	 * 表示对数据库的写操作。注意，这样的写操作对read()不应该可见。
	 * 调用，直到执行写操作的事务提交为止。为了简单起见，我们将不进行读取
	 * 在我们写入该键后，从txID本身到相同的键。
	 */
	public void write(long txID, long key, byte[] value) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));

		//记录写操作日志到RecordMap中
		ArrayList<Record> records = RecordMap.get(txID);
		if (records == null) {
			records = new ArrayList<>();
			RecordMap.put(txID, records);
		}
		records.add(new Record((byte)0,txID,key,value));
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 * 提交一个事务，并且使其写操作对后续的读操作可见
	 */
	public void commit(long txID) {
		//获取该事务先前进行的所有写操作日志records
		ArrayList<Record> records = RecordMap.get(txID);
		if (records==null){
			return;
		}
		// 把事务txID的提交(commit=1)Record写到records中
		records.add(new Record(txID, (byte) 1));

		//记录key和对应写入在lm中的尾巴最大偏移量tag
		Map<Long,Integer> keyToTag=new HashMap<>();
		//写入LogManager
		for (Record record : records) {
			lm.appendLogRecord(record.serialize());
			int tag = lm.getLogEndOffset();
			if (record.type==0){
				keyToTag.put(record.key,tag);
			}
		}

		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				//获取在日志中的偏移量tag
				Integer tag = keyToTag.get(x.key);
				//更新latestValues
				latestValues.put(x.key, new TaggedValue(tag, x.value));
				//写入StorageManager
				sm.queueWrite(x.key,tag,x.value);
				persisted.add(tag);
			}
			writesets.remove(txID);
		}
	}
	/**
	 * Aborts a transaction.
	 * 撤销事务
	 */
	public void abort(long txID) {
		RecordMap.remove(txID);
		writesets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
//		int logTruncationOffset = lm.getLogTruncationOffset();

//		OffsetSet.add((int)persisted_tag);
//		while (!OffsetSet.isEmpty()){
//			Integer min = OffsetSet.first();
//			if (min==logTruncationOffset){
//				byte[] bytes = lm.readLogRecord(min, Integer.BYTES);
//				ByteBuffer bb = ByteBuffer.wrap(bytes);
//				int len=bb.getInt();
//				lm.setLogTruncationOffset(logTruncationOffset+len);
//				OffsetSet.pollFirst();
//			}else break;
//		}

		//添加persisted_tag到OffsetSet中
		OffsetSet.add((int)persisted_tag);
		//每次提取已持久化日志的最小偏移量且是OffsetSet中存在的，进行setLogTruncationOffset;
		while (!persisted.isEmpty()&&OffsetSet.contains(persisted.peek())){
			long tag = persisted.poll();
			OffsetSet.remove((int)tag);
			lm.setLogTruncationOffset((int) tag);
		}
	}
}
