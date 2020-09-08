#ifndef _BUFFERMANAGER_H_
#define _BUFFERMANAGER_H_

/* Copyright 2020 Minzhe Tang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>
#include <cstdio>
#include <string>
#include <set>
#include <map>
#include <vector>
#include <list>
#include "Warning.h"
using namespace std;

#define MAX_BLOCK_NUM 8192      // 程序所能持有的最大块数量。8192 * 8 / 1024 = 64 (MB)
#define BLOCK_SIZE 8192         // 一个块在文件中占用8KB

//类前项声明
class BufferManager;
class IndexBlock;
class RecordBlock;

class FileInfo {
protected:
	const int type;			    	// 0 -> 数据文件, 1 -> 索引文件
	const string fileName;	    	// 文件名，包含后缀
	int blockNum = 0;               // 磁盘中此文件拥有的块数量，需要写入磁盘
public:
	// 构造函数
	// 参数
	// type    文件类型。0 -> 数据文件, 1 -> 索引文件
	// fileName    文件名，包含后缀
	FileInfo(int type, string fileName) :type(type), fileName(fileName) {}
};

class RecordFile : public FileInfo {
	friend class BufferManager;
	friend class RecordBlock;
private:
	const int RECORD_LEN;	    			// 元组长度，需写入文件。删除标志位不计入长度。当存在字符串属性时，需要计入结束符位。
	const int MAX_BLOCK_RECORD_NUM;			// 一个块中最多允许存放的元组数目，需写入文件
											// 写入磁盘的字典信息的顺序：blockNum->RECORD_LEN->MAX_BLOCK_RECORD_NUM
	const int FIRST_BLOCK_OFFSET = 12;		// 字典信息占用12字节。
	set<RecordBlock*> blocksInMemory;		// 已存在于内存中的属于当前文件的块的指针
	set<int> freeSlotOffsets;				// 空闲元组槽的偏移量，需要写入单独文件
											// 依赖块使用者维护
public:
	// 构造函数
	// 参数
	// tableName    表名
	// RECORD_LEN    记录长度。不包含起始用于指示元组是否删除的指示位。若包含字符串型属性，RECORD_LEN应包含结束符位。
	RecordFile(string tableName, int RECORD_LEN) :FileInfo(0, tableName + ".data"),
		RECORD_LEN(RECORD_LEN), MAX_BLOCK_RECORD_NUM((8192 - 4) / (4 + RECORD_LEN)) {
	}
	// 返回记录长度。删除标志位不计入长度。
	int returnRecordLen() {
		return RECORD_LEN;
	}
	// 返回下一个空闲槽的偏移量
	// 填充空槽后，必须调用removeFreeSlot将空槽偏移量移除
	int returnNextFreeSlotOffset() {
		if (freeSlotOffsets.size() > 0) {
			set<int>::iterator it = freeSlotOffsets.begin();
			return *it;
		}
		else return -1;
	}
	// 移除一个空闲槽的偏移量
	// 参数offset是需要移除的偏移量
	bool removeFreeSlotOffset(int offset) {
		if (freeSlotOffsets.count(offset)) {
			freeSlotOffsets.erase(offset);
			return true;
		}
		else {
			offsetOutOfRangeWarning("RecordFile::removeFreeSlot");
			return false;
		}
	}
	// 插入一个空槽偏移量
	void insertFreeSlotOffset(int offset) {
		freeSlotOffsets.insert(offset);
	}
	// 告知传入偏移量对应的块是否在内存中，若在内存中则返回其指针，否则返回空指针
	// 参数    offset 块偏移量
	// 返回值    查询的块的指针或空指针
	// 注意    调用者必须保证offset合法性，因为无法通过返回值确定是否存在异常
	RecordBlock* isBlockInMemory(int offset);
};

class IndexFile : public FileInfo {
	friend class BufferManager;
	friend class IndexBlock;
private:

	const int INDEX_TYPE;					// 索引类型。1->整型，2->浮点型，3->字符串型。需要写入文件。
	const int KEY_LEN;						// 键值的长度，需要写入文件。当索引为字符串型时，该值为字符数组长度。实际能存储的字符串长度为KEY_LEN - 1.
	const int MAX_BLOCK_KEY_NUM;			// 一个块中最多允许存放多少个key，需要写入文件
	const int FIRST_BLOCK_OFFSET = 20;		// 字典信息占用20字节。
	int rootOffset = -1;					// 根在文件中的偏移量，没有根时置-1，需要写入文件
											// 写入磁盘的字典信息的顺序：blockNum->INDEX_TYPE->KEY_LEN->MAX_BLOCK_KEY_NUM->rootOffset
	set<IndexBlock*> blocksInMemory;		// 已存在于内存中的属于当前文件的块的指针
	set<int> emptyBlockOffsets;				// 空块的偏移量
public:
	// 构造函数
	// 参数
	// tableName    表名
	// attriName    属性名
	// INDEX_TYPE    索引类型。1->整型，2->浮点型，3->字符串型。
	// KEY_LEN    键长。若为字符串型索引，KEY_LEN应包含结束符位。
	IndexFile(string tableName, string attriName, int INDEX_TYPE, int KEY_LEN) :
		FileInfo(1, tableName + " - " + attriName + ".index"), INDEX_TYPE(INDEX_TYPE),
		KEY_LEN(KEY_LEN), MAX_BLOCK_KEY_NUM((8192 - 16) / (4 + KEY_LEN)) {
	}
	// 告知传入偏移量对应的块是否在内存中，若在内存中则返回其指针，否则返回空指针
	// 参数    offset 块偏移量
	// 注意    调用者必须保证offset合法性，因为无法通过返回值确定是否存在异常
	IndexBlock* isBlockInMemory(int offset);
	// 在读取块时调用此函数，确保传入位置是一个块首位置，并且此位置上确实有块存在
	bool isBlockOffsetValid(int offset) {
		if (offset >= FIRST_BLOCK_OFFSET + blockNum * BLOCK_SIZE || offset < FIRST_BLOCK_OFFSET) return false;
		if ((offset - FIRST_BLOCK_OFFSET) % BLOCK_SIZE != 0) return false;
		for (int i : emptyBlockOffsets) {
			if (i == offset) return false;
		}
		return true;
	}
};

class BlockInfo {
protected:
	const string fileName;      // 块所属的文件名
	const int id;				// 块的id，仅用于内存管理使用，不写入文件
	const int offset;           // 块的起始位置在文件中的偏移量
	bool isDirty = false;       // 指示块是否被修改过
	bool pinned = false;		// 指示块上是否有锁
public:
	// 构造函数
	// 参数
	// id    块ID
	// offset    块在文件中的偏移量
	// fileName    文件名，包含后缀
	BlockInfo(int id, int offset, string fileName) :id(id), offset(offset),
		fileName(fileName) {
	}
	// 设置块为脏块
	void setDirty() {
		isDirty = true;
	}
	// 返回块ID
	int returnID() {
		return id;
	}
	// 返回块在文件中的偏移量
	int returnOffset() {
		return offset;
	}
	// 解锁块
	void unpin() {
		pinned = false;
	}
};

class RecordBlock : public BlockInfo {
	friend class BufferManager;
private:
	// 从块所属的文件名解析表名
	string parseTableName();
public:
	//变量
	const int MAX_RECORD_NUM;           // 最多存放的元组数
	const int RECORD_LEN;				// 元组长度（不包含删除标记位）
	void* srcData = nullptr;			// 原始数据
	int recordNum = 0;                  // 当前存储的元组数，需要写入磁盘

	//函数
	// 构造函数
	// 初始化成员，同时将所有元组槽置为空闲。
	// 参数
	// id 块ID
	// offset    块在文件中的偏移量
	// tableName    表名
	// MAX_RECORD_NUM    最大记录数
	// 注意    不会为srcData分配空间
	RecordBlock(int id, int offset, string tableName, int MAX_RECORD_NUM,
		int RECORD_LEN) :BlockInfo(id, offset, tableName + ".data"),
		MAX_RECORD_NUM(MAX_RECORD_NUM), RECORD_LEN(RECORD_LEN) {
	}
	// 析构函数，释放srcData数据区
	~RecordBlock() {
		delete[] srcData;
	}
	// 返回块所属的文件对象的指针，从而块的使用者可以使用文件对象的公有函数
	RecordFile* returnFilePtr();
	// 将所有元组槽置为空闲
	// 一般在新建块时调用。调用者必须保证srcData已经分配空间
	void setAllSlotsAvailable();
};

class IndexBlock : public BlockInfo {
	friend class BufferManager;
private:
	//变量
	void* srcData = nullptr;			// 原始数据

	//函数
	// 从块所属的文件名解析表名和属性名
	// 参数
	// tableName    表名，函数将修改此参数
	// attriName    属性名，函数将修改此参数
	void parseTableAttriName(string& tableName, string& attriName);
	void parseData();									// 解析数据，用来将原始数据装填进ptrs和data数组
	void packData();									// 用于将更新的数据装填回srcData数组
														// 上级调用者freeIndexBlock保证块是脏块且未加锁
public:
	//变量
	const int TYPE;						// 索引类型。1->整型，2->浮点型，3->字符串型
	const int KEY_LEN;					// key长度。当索引为字符串型时，该值为字符数组长度。实际能存储的字符串长度为KEY_LEN - 1.
	const int MAX_KEY_NUM;              // 最多允许存储的key数
	int parent = -1;                    // 父节点指针，需要写入磁盘。默认值-1
										// 在磁盘中存储的字典信息，顺序为parent->nextSibling->keyNum
	int nextSibling = -1;				// 叶节点的下一个兄弟节点，需要写入磁盘。默认值-1
	int keyNum = 0;                     // 块中存储的key的数量，需要写入磁盘。默认值0
	int* ptrs = nullptr;                // 指针数组，实际是存储offset的数组
										// 在磁盘中，指针先于key存储
	int* keyi = nullptr;				// 整型key数组。最多允许1022个整型key。
	float* keyf = nullptr;				// 浮点key数组。最多允许1022个浮点型key。
	char** keystr = nullptr;			// 字符串型key数组，最大key数量动态确定。

	//函数
	// 将当前的块设置为根
	void setRoot();
	// 构造函数
	// 参数
	// TYPE 指示此索引的key为何种类型。1->整型，2->浮点型，3->字符串型
	// KEY_LEN 键长
	IndexBlock(int id, int offset, string tableName, string attriName,
		int TYPE, int KEY_LEN) :BlockInfo(id, offset, tableName + " - " + attriName + ".index"),
		TYPE(TYPE), KEY_LEN(KEY_LEN), MAX_KEY_NUM((8192 - 16) / (4 + KEY_LEN)) {
	}
	// 析构函数，确保原始数据区、指针区、键值区都被释放
	~IndexBlock() {
		delete[] srcData;
		delete[] ptrs;
		delete[] keyi;
		delete[] keyf;
		delete[] keystr;
	}
	// 返回块所属的文件对象的指针，从而块的使用者可以使用文件对象的公有函数
	IndexFile* returnFilePtr();
};

class BufferManager {
	friend class IndexBlock;
	friend class RecordBlock;
private:
	//变量
	//文件管理
	static map<string, IndexFile*> indexFilesOpened;				// 文件名到IndexFile对象的映射
	static map<string, RecordFile*> recordFilesOpened;              // 同上
	//块管理
	static int num_BlocksHeld;										// 程序当前持有的块的数量
	static list<int> usedTime;                                      // id组成的链表，每次解锁一个块后将其置于链表头，这样在链表尾的块就是最近最少使用块
	static set<int> freeIDs;                                        // 可用的块ID。需要新块时，首先从此向量中取出一个ID，没有空闲ID再分配新块或LRU
	static map<int, RecordBlock*> recordBlocksHeld;                 // 块id到块对象的映射
	static map<int, IndexBlock*> indexBlocksHeld;                   // 同上

	//函数
	// 打开文件，在内存中建立文件句柄
	// 参数    tableName 表名
	// 返回值    成功打开或句柄已存在返回true，否则返回false
	static bool openRecordFile(string tableName);
	// 打开文件，在内存中建立文件句柄
	// 参数
	// tableName    表名
	// atrriName    属性名
	// 返回值    成功打开或句柄已存在返回true，否则返回false
	static bool openIndexFile(string tableName, string attriName);
	// 写回属于此文件的脏块，释放内存中属于此文件的所有块，然后释放句柄
	// 参数    fileName 文件名，包含后缀名
	// 返回值    关闭成功或句柄不存在返回true，否则返回false
	// 注意    文件持有的块有锁时将导致关闭失败，忽略函数返回值可能导致严重后果
	static bool closeRecordFile(string fileName);
	// 写回属于此文件的脏块，释放内存中属于此文件的所有块，然后释放句柄
	// 参数    fileName 文件名，包含后缀名
	// 返回值    关闭成功或句柄不存在返回true，否则返回false
	// 注意    文件持有的块有锁时将导致关闭失败，忽略函数返回值可能导致严重后果
	static bool closeIndexFile(string fileName);
	// 强制释放内存中未加锁的块（无论是否是脏块），清除文件的内容，最后释放文件句柄
	// 参数    fileName 文件名，包含后缀名
	// 返回值    清除成功返回true，否则返回false
	// 注意    文件持有的块有锁时将导致清除失败，忽略函数返回值可能导致严重后果
	static bool clearRecordFile(string fileName);
	// 强制释放内存中未加锁的块（无论是否是脏块），清除文件的内容，最后释放文件句柄
	// 参数    fileName 文件名，包含后缀名
	// 返回值    清除成功返回true，否则返回false
	// 注意    文件持有的块有锁时将导致清除失败，忽略函数返回值可能导致严重后果
	static bool clearIndexFile(string fileName);
	// 强制释放块，无论是否是脏块。但不会释放有锁的块
	// 参数    id 块ID
	// 返回值    释放成功或块不存在返回true，否则返回false
	// 注意    函数不会将脏块写入磁盘
	static bool forceFreeRecordBlock(int id);
	// 强制释放块，无论是否是脏块。但不会释放有锁的块
	// 参数    id 块ID
	// 返回值    释放成功或块不存在返回true，否则返回false
	// 注意    函数不会将脏块写入磁盘
	static bool forceFreeIndexBlock(int id);
	// 返回一个新的记录块
	// 参数    tableName 表名
	// 返回值    新块的指针
	static RecordBlock* newRecordBlock(string tableName);
	// 读取一个记录块
	// 参数
	// tableName    表名
	// offset    块在文件中的偏移量
	// 返回值    块的指针。当块不存在时返回nullptr
	static RecordBlock* readRecordBlock(string tableName, int offset);
	// 向磁盘写入一个记录块
	// 参数    id 块ID
	// 返回值    写入成功返回true
	static bool writeRecordBlock(int id);
	// 释放一个记录块
	// 函数自动写回无锁脏块
	// 参数    id 块ID
	// 返回值    释放成功或块不存在返回true，否则返回false
	// 注意    块上有锁时将导致释放失败
	static bool freeRecordBlock(int id);
	// 返回一个新的索引块
	// 参数
	// tableName    表名
	// attriName    属性名
	// 返回值    新块的指针
	static IndexBlock* newIndexBlock(string tableName, string attriName);
	// 读取一个索引块
	// 参数
	// tableName    表名
	// attriName    属性名
	// offset    块在文件中的偏移量
	// 返回值    块的指针。当块不存在时返回nullptr
	static IndexBlock* readIndexBlock(string tableName, string attriName, int offset);
	// 向磁盘写入一个索引块
	// 参数    id 块ID
	// 返回值    写入成功返回true
	static bool writeIndexBlock(int id);
	// 释放一个索引块
	// 函数自动写回无锁脏块
	// 参数    id 块ID
	// 返回值    释放成功或块不存在返回true，否则返回false
	// 注意    块上有锁时将导致释放失败
	static bool freeIndexBlock(int id);
	// 返回最近最少使用的块的ID
	static int LRU();
	// 分配一个ID，用于载入新块
	// 若当前持有块的数量已经达到最大值，将使用LRU策略释放一个块
	static int allocateID();
	// 根据表名返回数据文件的文件名
	// 参数    tableName 表名
	// 返回值    数据文件的文件名
	static string getFileName(string tableName) {
		return tableName + ".data";
	}
	// 根据表名与属性名返回索引文件的文件名
	// 参数
	// tableName    表名
	// attriName    属性名
	// 返回值    索引文件的文件名
	static string getFileName(string tableName, string attriName) {
		return tableName + " - " + attriName + ".index";
	}
	// 根据数据或索引文件名返回对应的freelist文件名
	// 参数    fileName 数据或索引文件名
	// 返回值    freelist文件名
	static string getFreelistFileName(string fileName) {
		int pos = fileName.find('.');
		//trunk 主干
		int trunkNameLen = pos;
		return fileName.substr(0, trunkNameLen) + ".freelist";
	}
public:
	// 建立记录文件
	// 函数只建立文件句柄，不会在磁盘上实际产生文件
	// 参数
	// tableName    表名
	// recordLen    记录长度
	// 返回值    成功建立句柄返回true
	static bool createRecordFile(string tableName, int recordLen);
	// 建立索引文件
	// 函数只建立文件句柄，不会在磁盘上实际产生文件
	// 参数
	// tableName    表名
	// recordLen    记录长度
	// type    索引类型。1->整型，2->浮点型，3->字符串型
	// keyLen    键长
	// 返回值    成功建立句柄返回true
	static bool createIndexFile(string tableName, string attriName, int type, int keyLen = 4);
	// 强制释放所有块，清空表的所有记录和索引文件，最后释放文件句柄
	// 参数    tableName 表名
	// 返回值    删除成功返回true，删除失败或表不存在返回false
	// 注意    若存在任何一个属于此表的块上有锁，将导致删除失败。忽略函数返回值可能导致严重后果
	static bool dropTable(string tableName);
	// 删除索引，基于clearIndexFile实现
	// 参数
	// tableName    表名
	// attriName    加有索引的属性名
	// 返回值    清除成功返回true，属性不存在、未加索引、清除文件失败返回false
	static bool dropIndex(string tableName, string attriName);
	// 返回一个记录块
	// 参数
	// tableName    表名
	// offset    块在文件中的偏移量，可以是块中的任意位置，函数将根据此参数计算出块首的偏移量
	// 返回值    块的指针。表不存在，或传入位置上没有块时返回nullptr
	static RecordBlock* returnRecordBlock(string tableName, int offset);
	// 返回一个有空闲槽的记录块
	// 参数    tableName 表名
	// 返回值    块的指针
	static RecordBlock* returnFreeRecordBlock(string tableName);
	// 返回一个索引块
	// 参数
	// tableName    表名
	// AttriName    属性名
	// offset    块在文件中的偏移量
	// 返回值    块的指针。表或属性不存在，或传入位置上没有块时返回nullptr
	static IndexBlock* returnIndexBlock(string tableName, string attriName, int offset);
	// 返回根块
	// 参数
	// tableName    表名
	// AttriName    属性名
	// 返回值    根块的指针。表或属性不存在时返回nullptr
	static IndexBlock* returnRoot(string tableName, string attriName);
	// 返回一个新的索引块，基于newIndexBlock实现
	// 参数
	// tableName    表名
	// AttriName    属性名
	// 返回值    新块的指针。表或属性不存在时返回nullptr
	static IndexBlock* returnFreeIndexBlock(string tableName, string atrriName);
	// 更新块在usedTime双链表中的位置
	// 参数    id 块ID
	// 返回值    更新成功返回true，id不存在时返回false
	static bool updateUsedTime(int id);
	// 返回文件中当前块的下一个块，基于returnRecordBlock实现
	// 参数    curID 当前块的ID，函数将返回此块在文件中的下一个块
	// 返回值    下一个块的指针。内存中不存在ID为curID的块，或表不存在，或当前块是文件中的最后一个块时都将返回nullptr
	static RecordBlock* returnNextRecordBlock(int curID);
	// 返回文件的第一个块
	// 参数    tableName 表名
	// 返回值    第一个块的指针。表名非法或文件尚不存在块返回nullptr
	static RecordBlock* returnFirstRecordBlock(string tableName);
	// 删除B+树中的节点后，调用此函数来移除块。块在文件中占有的区域将用于下次建立新块
	// 参数    ids需要移除块的id的向量
	// 返回值    移除成功返回true。仅当传入在内存中不存在的id时返回false
	static bool removeIndexBlock(vector<int> ids);
	// 释放所有块和文件句柄，然后关闭Buffer Manager
	// 返回值    成功关闭返回true，否则返回false
	// 注意    Buffer Manager可能关闭失败，这可能是有块尚未解锁所致。忽略函数返回值可能导致严重后果
	static bool shutdown();
};

#endif