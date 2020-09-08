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

#include "BufferManager.h"
#include "CatalogManager.h"
#include "Warning.h"
#include <iostream>

//静态成员变量定义
map<string, IndexFile*> BufferManager::indexFilesOpened;
map<string, RecordFile*> BufferManager::recordFilesOpened;
int BufferManager::num_BlocksHeld = 0;
list<int> BufferManager::usedTime;
set<int> BufferManager::freeIDs;
map<int, RecordBlock*> BufferManager::recordBlocksHeld;
map<int, IndexBlock*> BufferManager::indexBlocksHeld;

RecordBlock* RecordFile::isBlockInMemory(int offset) {
	set<RecordBlock*>::iterator sit;
	for (sit = blocksInMemory.begin(); sit != blocksInMemory.end(); sit++) {
		if ((*sit)->returnOffset() == offset) return *sit;
	}
	return nullptr;
}

IndexBlock* IndexFile::isBlockInMemory(int offset) {
	set<IndexBlock*>::iterator sit;
	for (sit = blocksInMemory.begin(); sit != blocksInMemory.end(); sit++) {
		if ((*sit)->returnOffset() == offset) return *sit;
	}
	return nullptr;
}

RecordFile* RecordBlock::returnFilePtr() {
	return BufferManager::recordFilesOpened.at(fileName);
}

void RecordBlock::setAllSlotsAvailable() {
	const int mark = 0;
	char* datap = (char*)srcData;
	//跳过数据字典
	datap += 4;
	for (int i = 1; i <= MAX_RECORD_NUM; i++) {
		memcpy(datap, &mark, 4);
		datap += (RECORD_LEN + 4);
	}
}

string RecordBlock::parseTableName() {
	int pos = fileName.find('.');
	int tableNameLen = pos;
	return fileName.substr(0, tableNameLen);
}

void IndexBlock::parseTableAttriName(string& tableName, string& attriName) {
	int pos = fileName.find('-');
	int tableNameLen = pos - 1;
	//" - "长度为3，".index"长度为6
	int attriNameLen = fileName.length() - tableNameLen - 3 - 6;
	tableName = fileName.substr(0, tableNameLen);
	attriName = fileName.substr(pos + 2, attriNameLen);
}

void IndexBlock::parseData() {
	//读取parent，nextSibling和keyNum
	char* srcp = (char*)srcData;
	memcpy(&parent, srcp, 4);
	srcp += 4;
	memcpy(&nextSibling, srcp, 4);
	srcp += 4;
	memcpy(&keyNum, srcp, 4);
	srcp += 4;
	//获取索引类型
	string tableName, attriName;
	parseTableAttriName(tableName, attriName);
	const int type = returnIndexType(tableName, attriName);

	//解析数据
	//解析指针
	//按最大容量分配空间
	ptrs = new int[MAX_KEY_NUM + 1];
	int* ip = ptrs;
	for (int i = 1; i <= keyNum + 1; i++) {
		memcpy(ip, srcp, 4);
		ip++;
		srcp += 4;
	}
	//解析key
	//整型
	if (type == 1) {
		keyi = new int[MAX_KEY_NUM];
		ip = keyi;
		for (int i = 1; i <= keyNum; i++) {
			memcpy(ip, srcp, 4);
			ip++;
			srcp += 4;
		}
	}
	//浮点型
	else if (type == 2) {
		keyf = new float[MAX_KEY_NUM];
		float* flop = keyf;
		for (int i = 1; i <= keyNum; i++) {
			memcpy(flop, srcp, 4);
			flop++;
			srcp += 4;;
		}
	}
	//字符串型
	else {
		keystr = new char* [MAX_KEY_NUM];
		for (int i = 0; i <= MAX_KEY_NUM - 1; i++) {
			keystr[i] = new char[KEY_LEN];
		}
		for (int i = 0; i <= keyNum - 1; i++) {
			memcpy(keystr[i], srcp, KEY_LEN);
			srcp += KEY_LEN;
		}
	}

	delete[] srcData;
	//delete空指针不会引发异常
	//delete运算只释放空间，不会将指针赋空
	srcData = nullptr;
}

void IndexBlock::packData() {
	srcData = new char[8192];
	char* srcp = (char*)srcData;
	//打包字典信息
	memcpy(srcp, &parent, 4);
	srcp += 4;
	memcpy(srcp, &nextSibling, 4);
	srcp += 4;
	memcpy(srcp, &keyNum, 4);
	srcp += 4;
	//打包指针
	for (int i = 0; i <= keyNum; i++) {
		memcpy(srcp, ptrs, 4);
		srcp += 4;
		ptrs++;
	}
	delete[] ptrs;
	ptrs = nullptr;
	//打包数据
	//整型
	if (TYPE == 1) {
		for (int i = 0; i <= keyNum - 1; i++) {
			memcpy(srcp, keyi, 4);
			keyi++;
			srcp += 4;
		}
		delete[] keyi;
		keyi = nullptr;
	}
	//浮点型
	else if (TYPE == 2) {
		for (int i = 0; i <= keyNum - 1; i++) {
			memcpy(srcp, keyf, 4);
			keyf++;
			srcp += 4;
		}
		delete[] keyf;
		keyf = nullptr;
	}
	//字符串型
	else {
		for (int i = 0; i <= keyNum - 1; i++) {
			memcpy(srcp, keystr[i], KEY_LEN);
			srcp += KEY_LEN;
		}
		for (int i = 0; i <= MAX_KEY_NUM - 1; i++) {
			delete[] keystr[i];
			keystr[i] = nullptr;
		}
		delete[] keystr;
		keystr = nullptr;
	}
}

void IndexBlock::setRoot() {
	IndexFile* file = BufferManager::indexFilesOpened.at(fileName);
	file->rootOffset = offset;
}

IndexFile* IndexBlock::returnFilePtr() {
	return BufferManager::indexFilesOpened.at(fileName);
}

//类外实现成员函数不可声明为static：与作用域限定（函数仅可在本文件中调用）产生二义性
bool BufferManager::openRecordFile(string tableName) {
	string fileName = getFileName(tableName);
	//检查表的存在性
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::openRecordFile", tableName);
		return false;
	}
	//检查文件是否已打开
	if (recordFilesOpened.count(fileName)) return true;
	//尝试打开数据文件
	FILE* fp = fopen(fileName.c_str(), "rb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::openRecordFile", fileName);
		return false;
	}
	//读取字典信息
	int blockNum, recordLen, maxBlockRecordNum;
	fread(&blockNum, 4, 1, fp);
	fread(&recordLen, 4, 1, fp);
	fread(&maxBlockRecordNum, 4, 1, fp);
	fclose(fp);

	RecordFile* file = new RecordFile(tableName, recordLen);
	file->blockNum = blockNum;
	//尝试打开freelist文件
	string freelistFileName = tableName + ".freelist";
	fp = fopen(freelistFileName.c_str(), "rb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::openRecordFile", freelistFileName);
		delete file;
		return false;
	}
	//读取freelist
	while (!feof(fp)) {
		int offset;
		fread(&offset, 4, 1, fp);
		file->freeSlotOffsets.insert(offset);
	}
	fclose(fp);
	//文件对象指针加入recordFilesOpened映射
	recordFilesOpened.insert(pair<string, RecordFile*>(fileName, file));
	return true;
}

bool BufferManager::openIndexFile(string tableName, string attriName) {
	string fileName = getFileName(tableName, attriName);
	//检查表和属性存在性
	if (!doesAttriExist(tableName, attriName)) {
		string fileName = getFileName(tableName, attriName);
		attriNotExistWarning("BufferManager::openIndexFile", tableName, attriName);
		return false;
	}
	//检查文件是否已经打开
	if (indexFilesOpened.count(fileName)) return true;
	//尝试打开文件
	FILE* fp = fopen(fileName.c_str(), "rb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::openIndexFile", fileName);
		return false;
	}
	//读取字典信息
	int blockNum, indexType, keyLen, maxBlockKeyNum, rootOffset;
	fread(&blockNum, 4, 1, fp);
	fread(&indexType, 4, 1, fp);
	fread(&keyLen, 4, 1, fp);
	fread(&maxBlockKeyNum, 4, 1, fp);
	fread(&rootOffset, 4, 1, fp);
	fclose(fp);

	IndexFile* file = new IndexFile(tableName, attriName, indexType, keyLen);
	file->blockNum = blockNum;
	file->rootOffset = rootOffset;
	//尝试读入freelist信息
	string freelistFileName = getFreelistFileName(fileName);
	fp = fopen(fileName.c_str(), "rb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::openIndexFile", freelistFileName);
		delete file;
		return false;
	}
	while (!feof(fp)) {
		int offset;
		fread(&offset, 4, 1, fp);
		file->emptyBlockOffsets.insert(offset);
	}
	fclose(fp);
	//文件对象指针加入indexFilesOpened映射
	indexFilesOpened.insert(pair<string, IndexFile*>(fileName, file));
	return true;
}

bool BufferManager::createRecordFile(string tableName, int recordLen) {
	string fileName = getFileName(tableName);
	//检查表存在性
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::createRecordFile", tableName);
		return false;
	}
	//检查文件是否已打开
	if (recordFilesOpened.count(fileName)) return true;
	//检查是否文件已存在但未打开
	else {
		FILE* fp = fopen(fileName.c_str(), "r");
		if (fp != nullptr) {
			fclose(fp);
			return true;
		}
	}
	//创建一个无记录文件
	FILE* fp = fopen(fileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::createRecordFile", fileName);
		return false;
	}
	fclose(fp);
	//建立空freelist文件
	string freelistFileName = getFreelistFileName(fileName);
	fp = fopen(freelistFileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::createRecordFile", freelistFileName);
		return false;
	}
	fclose(fp);
	//建立文件对象并加入映射
	RecordFile* file = new RecordFile(tableName, recordLen);
	recordFilesOpened.insert(pair<string, RecordFile*>(fileName, file));
	return true;
}

//默认参数只能声明在函数声明与函数定义两者之一
bool BufferManager::createIndexFile(string tableName, string attriName, int type, int keyLen) {
	string fileName = getFileName(tableName, attriName);
	//检查属性存在性
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::createIndexFile", tableName, attriName);
		return false;
	}
	//检查文件是否已打开
	if (indexFilesOpened.count(fileName)) return true;
	//检查是否文件未打开但已存在
	else {
		FILE* fp = fopen(fileName.c_str(), "r");
		if (fp != nullptr) {
			fclose(fp);
			return true;
		}
	}
	//建立空索引文件
	FILE* fp = fopen(fileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::createIndexFile", fileName);
		return false;
	}
	fclose(fp);
	//建立空freelist文件
	string freelistFileName = getFreelistFileName(fileName);
	fp = fopen(freelistFileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::createIndexFile", freelistFileName);
		return false;
	}
	fclose(fp);
	//建立文件对象并加入映射
	IndexFile* file = new IndexFile(tableName, attriName, type, keyLen);
	indexFilesOpened.insert(pair<string, IndexFile*>(fileName, file));
	return true;
}

//新建一个数据块。函数先检查表的存在性和文件是否打开，表不存在时输出错误信息并返回空指针，文件未打开则调用openRecordFile。
//调用allocateID获取一个ID，根据文件对象的blockNum初始化块的offset，再初始化其他成员（包括分配数据区）。
//函数将对象加入recordBlocksHeld映射。函数还将更新文件对象的blockNum，blocksInMemory，freeSlotOffsets。
RecordBlock* BufferManager::newRecordBlock(string tableName) {
	//检查表的存在性
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::newRecordBlock", tableName);
		return nullptr;
	}
	//检查文件是否已打开
	string fileName = getFileName(tableName);
	if (!recordFilesOpened.count(fileName)) {
		if (!openRecordFile(tableName)) {
			functionFailureWarning("BufferManager::newRecordBlock", "BufferManager::openRecordFile");
			return nullptr;
		}
	}
	//文件成功打开，尝试申请id
	int id = allocateID();
	if (id == -1) {
		functionFailureWarning("BufferManager::newRecordBlock", "BufferManager::allocateID");
		return nullptr;
	}
	//建立新块并初始化
	RecordFile* file = recordFilesOpened.at(fileName);
	int offset = file->FIRST_BLOCK_OFFSET + file->blockNum * BLOCK_SIZE;
	int maxRecordNum = file->MAX_BLOCK_RECORD_NUM;
	int recordLen = file->RECORD_LEN;
	RecordBlock* block = new RecordBlock(id, offset, tableName, maxRecordNum, recordLen);
	block->srcData = new char[BLOCK_SIZE];
	block->setAllSlotsAvailable();

	//更新文件信息
	file->blockNum++;
	file->blocksInMemory.insert(block);
	int tupleOffset = offset + 4;
	for (int i = 1; i <= maxRecordNum; i++) {
		file->freeSlotOffsets.insert(tupleOffset);
		tupleOffset += 4 + recordLen;
	}

	//将块加入recordBlocksHeld映射
	recordBlocksHeld.insert(pair<int, RecordBlock*>(id, block));
	return block;
}

IndexBlock* BufferManager::newIndexBlock(string tableName, string attriName) {
	//检查表和属性的存在性
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::newIndexBlock", tableName, attriName);
		return nullptr;
	}
	//检查文件是否已打开
	string fileName = getFileName(tableName, attriName);
	if (!indexFilesOpened.count(fileName)) {
		if (!openIndexFile(tableName, attriName)) {
			functionFailureWarning("BufferManager::newIndexBlock", "BufferManager::openIndexFile");
			return nullptr;
		}
	}
	//文件成功打开，尝试申请id
	int id = allocateID();
	if (id == -1) {
		functionFailureWarning("BufferManager::newIndexBlock", "BufferManager::allocateID");
		return nullptr;
	}
	//建立新块并初始化
	IndexFile* file = indexFilesOpened.at(fileName);
	int offset;
	//优先使用文件中的空块
	if (file->emptyBlockOffsets.size() > 0) {
		offset = *(file->emptyBlockOffsets.begin());
		file->emptyBlockOffsets.erase(file->emptyBlockOffsets.begin());
	}
	else offset = file->FIRST_BLOCK_OFFSET + file->blockNum * BLOCK_SIZE;
	int indexType = file->INDEX_TYPE;
	int keyLen = file->KEY_LEN;
	IndexBlock* block = new IndexBlock(id, offset, tableName, attriName, indexType, keyLen);
	int maxKeyNum = block->MAX_KEY_NUM;
	block->ptrs = new int[maxKeyNum + 1];
	if (indexType == 1) block->keyi = new int[maxKeyNum];
	else if (indexType == 2) block->keyf = new float[maxKeyNum];
	else {
		block->keystr = new char* [maxKeyNum];
		for (int i = 0; i <= maxKeyNum - 1; i++) {
			block->keystr[i] = new char[keyLen];
		}
	}

	//更新文件信息
	file->blockNum++;
	file->blocksInMemory.insert(block);

	//将块加入indexBlocksHeld映射
	indexBlocksHeld.insert(pair<int, IndexBlock*>(id, block));
	return block;
}

//返回一个可用的块ID. 
//函数从usedTime映射的开头向后检查，找到第一个未加锁的非脏块，调用freeBlock函数将块释放。
//freeBlock失败则continue. freeBlock函数会将块id加入freeIDs向量并让num_BlocksHeld自减，
//因而LRU函数需要再逻辑撤销这些操作。如果遍历usedTime链表都无法找到未加锁的非脏块，
//则重新遍历，此时只寻找未加锁的块，调用freeBlock将块释放。freeBlock失败则continue，直到成功释放一个块。
//如所有块都加锁将返回-1. 该函数仅用于返回一个可用的块ID，不会在堆上建立新块。
int BufferManager::LRU() {
	list<int>::reverse_iterator rit;
	int id;
	//尝试释放一个未加锁的非脏块
	for (rit = usedTime.rbegin(); rit != usedTime.rend(); rit++) {
		id = *rit;
		//数据块
		if (recordBlocksHeld.count(id)) {
			RecordBlock* block = recordBlocksHeld.at(id);
			//未加锁的非脏块，可以释放
			if (!(block->isDirty) && !(block->pinned)) {
				//释放成功
				if (freeRecordBlock(id)) {
					set<int>::iterator sit;
					for (sit = freeIDs.begin(); sit != freeIDs.end(); sit++) {
						if (*sit == id) {
							freeIDs.erase(sit);
							break;
						}
					}
					num_BlocksHeld++;
					return id;
				}
				else {
					functionFailureWarning("BufferManager::LRU", "BufferManager::freeRecordBlock");
					continue;
				}
			}
			else continue;
		}
		//索引块
		else if (indexBlocksHeld.count(id)) {
			IndexBlock* block = indexBlocksHeld.at(id);
			//未加锁的非脏块，可以释放
			if (!(block->isDirty) && !(block->pinned)) {
				//释放成功
				if (freeIndexBlock(id)) {
					set<int>::iterator sit;
					for (sit = freeIDs.begin(); sit != freeIDs.end(); sit++) {
						if (*sit == id) {
							freeIDs.erase(sit);
							break;
						}
					}
					num_BlocksHeld++;
					return id;
				}
				else {
					functionFailureWarning("BufferManager::LRU", "BufferManager::freeIndexBlock");
					continue;
				}
			}
			else continue;
		}
		//usedTime中出现blocksHeld中不存在的id，输出错误信息
		else {
			internalErrorWarning("BufferManager::LRU", "Inconsistency between usedTime list and blocksHeld map.");
			continue;
		}
	}
	//尝试释放一个未加锁的块
	for (rit = usedTime.rbegin(); rit != usedTime.rend(); rit++) {
		id = *rit;
		//数据块
		if (recordBlocksHeld.count(id)) {
			RecordBlock* block = recordBlocksHeld.at(id);
			//未加锁的块，尝试释放
			if (!block->pinned) {
				//释放成功
				if (freeRecordBlock(id)) {
					set<int>::iterator sit;
					for (sit = freeIDs.begin(); sit != freeIDs.end(); sit++) {
						if (*sit == id) {
							freeIDs.erase(sit);
							break;
						}
					}
					num_BlocksHeld++;
					return id;
				}
				else {
					functionFailureWarning("BufferManager::LRU", "BufferManager::freeRecordBlock");
					continue;
				}
			}
			else continue;
		}
		//索引块
		else if (indexBlocksHeld.count(id)) {
			IndexBlock* block = indexBlocksHeld.at(id);
			//未加锁的块，尝试释放
			if (!block->pinned) {
				//释放成功
				if (freeIndexBlock(id)) {
					set<int>::iterator sit;
					for (sit = freeIDs.begin(); sit != freeIDs.end(); sit++) {
						if (*sit == id) {
							freeIDs.erase(sit);
							break;
						}
					}
					num_BlocksHeld++;
					return id;
				}
				else {
					functionFailureWarning("BufferManager::LRU", "BufferManager::freeIndexBlock");
					continue;
				}
			}
			else continue;
		}
		//usedTime中出现blocksHeld中不存在的id，输出错误信息
		else {
			internalErrorWarning("BufferManager::LRU", "Inconsistency between usedTime list and blocksHeld map.");
			continue;
		}
	}
	//所有8192个块全部加锁，返回-1
	return -1;
}

//用于返回一个可用的块ID.函数先检查freeIDs集合中是否还有元素，如有则选取一个返回，
//否则需要检查num_BlocksHeld是否已达到最大值。如果没有，新分配一个ID（ = num_BlocksHeld + 1）然后返回。
//以上两种情形都需要让num_BlocksHeld自增。
//如果已经达到最大值，调用LRU来释放一个块，并返回LRU的返回值。
int BufferManager::allocateID() {
	//尝试从freeIDs中取出一个id
	if (!freeIDs.empty()) {
		set<int>::iterator sit = freeIDs.begin();
		int ret = *sit;
		freeIDs.erase(sit);
		num_BlocksHeld++;
		return ret;
	}
	//当前持有块数量未达到最大值，新分配一个id
	if (num_BlocksHeld < MAX_BLOCK_NUM) {
		num_BlocksHeld++;
		return num_BlocksHeld;
	}
	//当前持有块数量已经达到最大值，调用LRU
	return LRU();
}

//函数先为块加锁，更新数据区中的recordNum数据位，然后依据块的fileName和offset信息，
//调用fwrite向文件中写入块，但不会释放块。随后，函数将isDirty设置为false，将块解锁。写入成功将返回true.
bool BufferManager::writeRecordBlock(int id) {
	RecordBlock* block = recordBlocksHeld.at(id);
	block->pinned = true;
	//更新数据区内recordNum位
	memcpy(block->srcData, &(block->recordNum), 4);
	//尝试打开文件并写入数据
	FILE* fp = fopen(block->fileName.c_str(), "rb+");
	if (fp == nullptr) {
		block->pinned = false;
		return false;
	}
	fseek(fp, block->offset, SEEK_SET);
	fwrite(block->srcData, BLOCK_SIZE, 1, fp);
	fclose(fp);

	block->isDirty = false;
	block->pinned = false;
	return true;
}

bool BufferManager::writeIndexBlock(int id) {
	IndexBlock* block = indexBlocksHeld.at(id);
	block->pinned = true;
	//打包数据
	block->packData();
	//尝试打开文件并写入数据
	FILE* fp = fopen(block->fileName.c_str(), "rb+");
	if (fp == nullptr) {
		block->pinned = false;
		return false;
	}
	fseek(fp, block->offset, SEEK_SET);
	fwrite(block->srcData, BLOCK_SIZE, 1, fp);
	fclose(fp);

	block->isDirty = false;
	block->pinned = false;
	return true;
}

//函数释放内存中的块，如果是脏块，函数自动将其写回。
//函数检查块是否在内存中、是否是脏块、是否加锁，不在内存中的块直接返回true，加锁的块直接返回false，
//未加锁的脏块调用writeRecordBlock写回。若成功写回或无需写回（未加锁的非脏块），函数将id加入freeIDs集合，
//将id从usedTime链表和recordBlocksHeld映射移除，令num_BlocksHeld自减，释放块对象（析构函数确保数据区被释放），
//并将块指针从文件的blocksInMemory向量移除。
bool BufferManager::freeRecordBlock(int id) {
	//检查块的存在性、是否加锁、是否脏块（脏块自动写回）
	if (!recordBlocksHeld.count(id)) return true;
	RecordBlock* block = recordBlocksHeld.at(id);
	if (block->pinned) return false;
	if (block->isDirty) {
		if (!writeRecordBlock(id)) return false;
	}
	//此时得到的一定是未加锁的非脏块，执行释放步骤
	freeIDs.insert(id);

	list<int>::iterator lit;
	for (lit = usedTime.begin(); lit != usedTime.end(); lit++) {
		if (*lit == id) {
			usedTime.erase(lit);
			break;
		}
	}

	RecordFile* file = recordFilesOpened.at(block->fileName);
	set<RecordBlock*>::iterator sit;
	for (sit = file->blocksInMemory.begin(); sit != file->blocksInMemory.end(); sit++) {
		if ((*sit)->id == id) {
			file->blocksInMemory.erase(sit);
			break;
		}
	}

	map<int, RecordBlock*>::iterator mit;
	for (mit = recordBlocksHeld.begin(); mit != recordBlocksHeld.end(); mit++) {
		if (mit->first == id) {
			recordBlocksHeld.erase(mit);
			break;
		}
	}

	num_BlocksHeld--;

	delete block;
	return true;
}

bool BufferManager::freeIndexBlock(int id) {
	//检查块的存在性、是否加锁、是否脏块（脏块自动写回）
	if (!indexBlocksHeld.count(id)) return true;
	IndexBlock* block = indexBlocksHeld.at(id);
	if (block->pinned) return false;
	if (block->isDirty) {
		if (!writeIndexBlock(id)) return false;
	}
	//此时得到的一定是未加锁的非脏块，执行释放步骤
	freeIDs.insert(id);

	list<int>::iterator lit;
	for (lit = usedTime.begin(); lit != usedTime.end(); lit++) {
		if (*lit == id) {
			usedTime.erase(lit);
			break;
		}
	}

	IndexFile* file = indexFilesOpened.at(block->fileName);
	set<IndexBlock*>::iterator sit;
	for (sit = file->blocksInMemory.begin(); sit != file->blocksInMemory.end(); sit++) {
		if ((*sit)->id == id) {
			file->blocksInMemory.erase(sit);
			break;
		}
	}

	map<int, IndexBlock*>::iterator mit;
	for (mit = indexBlocksHeld.begin(); mit != indexBlocksHeld.end(); mit++) {
		if (mit->first == id) {
			indexBlocksHeld.erase(mit);
			break;
		}
	}

	num_BlocksHeld--;

	delete block;
	return true;
}

//函数从文件中读取块。
//函数需检查表是否存在、文件是否打开、传入位置上是否真的有块存在、块是否已在内存中。调用者保证offset恰为块的起始地址。
//调用allocateID得到一个可用的ID，新建一个对象，为ID赋值，并加入recordBlocksHeld映射。
//函数为offset，fileName，MAX_RECORD_NUM赋值，为data分配空间并读入数据，读取recordNum，其余赋默认值。
//函数还需将块的指针加入文件对象的blocksInMemory向量。
RecordBlock* BufferManager::readRecordBlock(string tableName, int offset) {
	//检查表的存在性
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::readRecordBlock", tableName);
		return nullptr;
	}
	//检查文件是否已经打开
	string fileName = getFileName(tableName);
	if (!recordFilesOpened.count(fileName)) {
		if (!openRecordFile(tableName)) {
			functionFailureWarning("BufferManager::readRecordBlock", "BufferManager::openRecordFile");
			return nullptr;
		}
	}
	//检查偏移量的合法性
	RecordFile* file = recordFilesOpened.at(fileName);
	if (offset >= file->FIRST_BLOCK_OFFSET + file->blockNum * BLOCK_SIZE || offset < file->FIRST_BLOCK_OFFSET) {
		offsetOutOfRangeWarning("BufferManager::readRecordBlock");
		return nullptr;
	}
	//检查块是否已在内存中
	set<RecordBlock*>::iterator sit;
	for (sit = file->blocksInMemory.begin(); sit != file->blocksInMemory.end(); sit++) {
		if ((*sit)->offset == offset) return *sit;
	}
	//分配ID
	int id = allocateID();
	if (id == -1) {
		functionFailureWarning("BufferManager::readRecordBlock", "BufferManager::allocateID");
		return nullptr;
	}
	//成功分配ID，新建块对象
	RecordBlock* block = new RecordBlock(id, offset, tableName, file->MAX_BLOCK_RECORD_NUM, file->RECORD_LEN);
	FILE* fp = fopen(fileName.c_str(), "rb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::readRecordBlock", fileName);
		delete block;
		return nullptr;
	}
	fseek(fp, offset, SEEK_SET);
	block->srcData = new char[BLOCK_SIZE];
	fread(block->srcData, BLOCK_SIZE, 1, fp);
	fclose(fp);
	memcpy(&(block->recordNum), block->srcData, 4);
	recordBlocksHeld.insert(pair<int, RecordBlock*>(id, block));
	//更新文件
	file->blocksInMemory.insert(block);
	return block;
}

IndexBlock* BufferManager::readIndexBlock(string tableName, string attriName, int offset) {
	//检查属性存在性
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::readIndexBlock", tableName, attriName);
		return nullptr;
	}
	//检查文件是否已经打开
	string fileName = getFileName(tableName, attriName);
	if (!indexFilesOpened.count(fileName)) {
		if (!openIndexFile(tableName, attriName)) {
			functionFailureWarning("BufferManager::readIndexBlock", "BufferManager::openIndexFile");
			return nullptr;
		}
	}
	//检查偏移量的合法性
	IndexFile* file = indexFilesOpened.at(fileName);
	if (!file->isBlockOffsetValid(offset)) {
		offsetOutOfRangeWarning("BufferManager::readIndexBlock");
		return nullptr;
	}
	//检查块是否已在内存中
	set<IndexBlock*>::iterator sit;
	for (sit = file->blocksInMemory.begin(); sit != file->blocksInMemory.end(); sit++) {
		if ((*sit)->offset == offset) return *sit;
	}
	//分配ID
	int id = allocateID();
	if (id == -1) {
		functionFailureWarning("BufferManager::readIndexBlock", "BufferManager::allocateID");
		return nullptr;
	}
	//成功分配ID，新建块对象
	IndexBlock* block = new IndexBlock(id, offset, tableName, attriName, file->INDEX_TYPE, file->KEY_LEN);
	FILE* fp = fopen(fileName.c_str(), "rb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::readIndexBlock", fileName);
		delete block;
		return nullptr;
	}
	fseek(fp, offset, SEEK_SET);
	block->srcData = new char[BLOCK_SIZE];
	fread(block->srcData, BLOCK_SIZE, 1, fp);
	fclose(fp);
	block->parseData();
	//更新文件
	file->blocksInMemory.insert(block);
	return block;
}

//由clearRecordFile调用，该函数保证块在内存中。
//无视块是否是脏块，强制将其释放，但仍然受到锁限制。
//函数首先检查块是否加锁，加锁的块直接返回false。
//上述检查通过后，释放srcData数据区，将id从recordBlocksHeld映射移除，令num_BlocksHeld自减，从usedTime链表移除id，
//将id加入freeIDs集合，并将块指针从文件对象的blocksInMemory集合移除。成功释放后返回true。
bool BufferManager::forceFreeRecordBlock(int id) {
	RecordBlock* block = recordBlocksHeld.at(id);
	if (block->pinned) {
		blockPinnedWarning("BufferManager::forceFreeRecordBlock", block->fileName);
		return false;
	}

	//从BufferManager缓存中清除块的信息
	list<int>::iterator lit;
	for (lit = usedTime.begin(); lit != usedTime.end(); lit++) {
		if (*lit == id) {
			usedTime.erase(lit);
			break;
		}
	}
	map<int, RecordBlock*>::iterator mit;
	for (mit = recordBlocksHeld.begin(); mit != recordBlocksHeld.end(); mit++) {
		if (mit->first == id) {
			recordBlocksHeld.erase(mit);
			break;
		}
	}
	num_BlocksHeld--;
	freeIDs.insert(id);

	//更新文件
	set<RecordBlock*>::iterator sit;
	RecordFile* file = recordFilesOpened.at(block->fileName);
	for (sit = file->blocksInMemory.begin(); sit != file->blocksInMemory.end(); sit++) {
		if (*sit == block) {
			file->blocksInMemory.erase(sit);
			break;
		}
	}

	delete block;
	return true;
}

bool BufferManager::forceFreeIndexBlock(int id) {
	IndexBlock* block = indexBlocksHeld.at(id);
	if (block->pinned) {
		blockPinnedWarning("BufferManager::forceFreeIndexBlock", block->fileName);
		return false;
	}

	//从BufferManager缓存中清除块的信息
	list<int>::iterator lit;
	for (lit = usedTime.begin(); lit != usedTime.end(); lit++) {
		if (*lit == id) {
			usedTime.erase(lit);
			break;
		}
	}
	map<int, IndexBlock*>::iterator mit;
	for (mit = indexBlocksHeld.begin(); mit != indexBlocksHeld.end(); mit++) {
		if (mit->first == id) {
			indexBlocksHeld.erase(mit);
			break;
		}
	}
	num_BlocksHeld--;
	freeIDs.insert(id);

	//更新文件
	set<IndexBlock*>::iterator sit;
	IndexFile* file = indexFilesOpened.at(block->fileName);
	for (sit = file->blocksInMemory.begin(); sit != file->blocksInMemory.end(); sit++) {
		if (*sit == block) {
			file->blocksInMemory.erase(sit);
			break;
		}
	}

	delete block;
	return true;
}

//由dropTable调用，该函数保证文件存在。
//先调用forceFreeRecordBlock逐个释放文件在内存中所持的块，文件不在内存中时跳过此步。
//函数检查forceFreeRecordBlock的返回值，如果有某次调用返回false，则文件删除失败，函数返回false。
//之后用有文件头但没有实际数据的文件替换原数据文件。最后将文件对象从openedRecordFiles映射中移除并释放文件句柄（如果已打开）。
bool BufferManager::clearRecordFile(string fileName) {
	//先检查文件是否在内存中
	if (recordFilesOpened.count(fileName)) {
		//逐个释放内存中的块
		RecordFile* file = recordFilesOpened.at(fileName);
		while (!file->blocksInMemory.empty()) {
			//函数forceFreeRecordBlock会从blocksInMemory集合中移除指针，无需重复移除
			if (forceFreeRecordBlock((*(file->blocksInMemory.begin()))->id)) continue;
			else {
				functionFailureWarning("BufferManager::clearRecordFile", "BufferManager::forceFreeRecordBlock");
				return false;
			}
		}
	}
	//清除文件内容
	FILE* fp = fopen(fileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::clearRecordFile", fileName);
		return false;
	}
	fclose(fp);
	string freelistFileName = getFreelistFileName(fileName);
	fp = fopen(freelistFileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::clearRecordFile", freelistFileName);
		return false;
	}
	fclose(fp);
	//移除文件对象并释放句柄
	if (recordFilesOpened.count(fileName)) {
		RecordFile* file = recordFilesOpened.at(fileName);
		map<string, RecordFile*>::iterator mit = recordFilesOpened.find(fileName);
		recordFilesOpened.erase(mit);
		delete file;
	}

	return true;
}

bool BufferManager::clearIndexFile(string fileName) {
	//先检查文件是否在内存中
	if (indexFilesOpened.count(fileName)) {
		//逐个释放内存中的块
		IndexFile* file = indexFilesOpened.at(fileName);
		while (!file->blocksInMemory.empty()) {
			//函数forceFreeIndexBlock会从blocksInMemory集合中移除指针，无需重复移除
			if (forceFreeIndexBlock((*(file->blocksInMemory.begin()))->id)) continue;
			else {
				functionFailureWarning("BufferManager::clearIndexFile", "BufferManager::forceFreeIndexBlock");
				return false;
			}
		}
	}
	//清除文件内容
	FILE* fp = fopen(fileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::clearIndexFile", fileName);
		return false;
	}
	fclose(fp);
	//清除freelist
	string freelistFileName = getFreelistFileName(fileName);
	fp = fopen(freelistFileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::clearIndexFile", freelistFileName);
		return false;
	}
	fclose(fp);
	//移除文件对象
	if (indexFilesOpened.count(fileName)) {
		IndexFile* file = indexFilesOpened.at(fileName);
		map<string, IndexFile*>::iterator mit = indexFilesOpened.find(fileName);
		indexFilesOpened.erase(mit);
		delete file;
	}

	return true;
}

//由shutdown函数调用，该函数保证传入文件的句柄一定已经存在。
//利用blocksInMemory集合逐个检查当前文件在内存中持有的块，用freeRecordBlock逐个释放，
//（当有块释放失败时，文件关闭失败，函数返回false）。
//之后函数向文件写入文件头的一些信息，并向另一个单独的文件写入freelist信息。
//最后函数将对象从openedRecordFiles映射移除并释放文件句柄。
bool BufferManager::closeRecordFile(string fileName) {
	RecordFile* file = recordFilesOpened.at(fileName);
	//释放块
	while (!file->blocksInMemory.empty()) {
		if (freeRecordBlock((*(file->blocksInMemory.begin()))->id)) continue;
		else {
			functionFailureWarning("BufferManager::closeRecordFile", "BufferManager::freeRecordBlock");
			return false;
		}
	}
	//更新文件头
	FILE* fp = fopen(fileName.c_str(), "rb+");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::closeRecordFile", fileName);
		return false;
	}
	fwrite(&(file->blockNum), 4, 1, fp);
	//固定值也重新写一遍，确保新建立文件的文件头能够被写入磁盘
	fwrite(&(file->RECORD_LEN), 4, 1, fp);
	fwrite(&(file->MAX_BLOCK_RECORD_NUM), 4, 1, fp);
	fclose(fp);
	//解析freelist文件名
	string freelistFileName = getFreelistFileName(fileName);
	//写入freelist
	fp = fopen(freelistFileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::closeRecordFile", freelistFileName);
		return false;
	}
	set<int>::iterator sit;
	for (sit = file->freeSlotOffsets.begin(); sit != file->freeSlotOffsets.end(); sit++) {
		fwrite(&(*sit), 4, 1, fp);
	}
	fclose(fp);
	//移除文件对象指针并释放文件对象
	map<string, RecordFile*>::iterator mit = recordFilesOpened.find(fileName);
	recordFilesOpened.erase(mit);
	delete file;

	return true;
}

bool BufferManager::closeIndexFile(string fileName) {
	IndexFile* file = indexFilesOpened.at(fileName);
	//释放块
	while (!file->blocksInMemory.empty()) {
		if (freeIndexBlock((*(file->blocksInMemory.begin()))->id)) continue;
		else {
			functionFailureWarning("BufferManager::closeIndexFile", "BufferManager::freeIndexBlock");
			return false;
		}
	}
	//更新文件头
	FILE* fp = fopen(fileName.c_str(), "rb+");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::closeIndexFile", fileName);
		return false;
	}
	fwrite(&(file->blockNum), 4, 1, fp);
	fwrite(&(file->INDEX_TYPE), 4, 1, fp);
	fwrite(&(file->KEY_LEN), 4, 1, fp);
	fwrite(&(file->MAX_BLOCK_KEY_NUM), 4, 1, fp);
	fwrite(&(file->rootOffset), 4, 1, fp);
	fclose(fp);
	//写freelist
	string freelistFileName = getFreelistFileName(fileName);
	fp = fopen(freelistFileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::closeIndexFile", freelistFileName);
		return false;
	}
	for (int offset : file->emptyBlockOffsets) {
		fwrite(&offset, 4, 1, fp);
	}
	fclose(fp);
	//移除文件对象指针并释放文件对象
	map<string, IndexFile*>::iterator mit = indexFilesOpened.find(fileName);
	indexFilesOpened.erase(mit);
	delete file;

	return true;
}

//用于依据传入的表名和offset返回块。
//函数不要求offset恰为块的起始偏移量，而是可以依据传入偏移量推算出所属块的偏移量，再将其取回。
//函数先检查表的存在性，再检查文件是否已打开，之后检查传入的位置上是否确实有块存在（依据blockNum）。
//之后，函数检查块是否已在内存中，若存在则直接返回，否则调用readRecordBlock从文件读取。
//函数自动为块加锁。
RecordBlock* BufferManager::returnRecordBlock(string tableName, int offset) {
	//检查表的存在性
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::returnRecordBlock", tableName);
		return nullptr;
	}
	//检查文件是否打开
	string fileName = getFileName(tableName);
	if (!recordFilesOpened.count(fileName)) {
		if (!openRecordFile(tableName)) {
			functionFailureWarning("BufferManager::returnRecordBlock", "BufferManager::openRecordFile");
			return nullptr;
		}
	}
	//计算块首偏移量
	RecordFile* file = recordFilesOpened.at(fileName);
	offset = (offset - file->FIRST_BLOCK_OFFSET) / BLOCK_SIZE * BLOCK_SIZE + file->FIRST_BLOCK_OFFSET;
	//检查偏移量合法性
	if (offset < file->FIRST_BLOCK_OFFSET || offset >= file->FIRST_BLOCK_OFFSET + file->blockNum * BLOCK_SIZE) {
		offsetOutOfRangeWarning("BufferManager::returnRecordBlock");
		return nullptr;
	}
	//尝试从内存中读取块
	RecordBlock* block = file->isBlockInMemory(offset);
	if (block != nullptr) return block;
	//从文件读取块
	block = readRecordBlock(tableName, offset);
	if (block == nullptr) {
		functionFailureWarning("BufferManager::returnRecordBlock", "BufferManager::readRecordBlock");
		return nullptr;
	}
	//自动加锁
	block->pinned = true;
	return block;
}

IndexBlock* BufferManager::returnIndexBlock(string tableName, string attriName, int offset) {
	//检查属性的存在性
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::returnIndexBlock", tableName, attriName);
		return nullptr;
	}
	//检查文件是否打开
	string fileName = getFileName(tableName, attriName);
	if (!indexFilesOpened.count(fileName)) {
		if (!openIndexFile(tableName, attriName)) {
			functionFailureWarning("BufferManager::returnIndexBlock", "BufferManager::openIndexFile");
			return nullptr;
		}
	}
	//检查偏移量合法性
	IndexFile* file = indexFilesOpened.at(fileName);
	if (!file->isBlockOffsetValid(offset)) {
		offsetOutOfRangeWarning("BufferManager::returnIndexBlock");
		return nullptr;
	}
	//尝试从内存中读取块
	IndexBlock* block = file->isBlockInMemory(offset);
	if (block != nullptr) return block;
	//从文件读取块
	block = readIndexBlock(tableName, attriName, offset);
	if (block == nullptr) {
		functionFailureWarning("BufferManager::returnIndexBlock", "BufferManager::readIndexBlock");
		return nullptr;
	}
	//自动加锁
	block->pinned = true;
	return block;
}

//函数返回一个空闲块。
//函数先检查表的存在性。其次检查记录文件是否已打开，若未打开调用openRecordFile打开文件。
//利用openedRecordFiles映射定位RecordFile对象，检查freeSlotOffsets中是否有元素。
//若有，逐个检查blocksInMemory向量中的各个块，看是否有块的offset符合，以及该块是否有锁，
//若有符合条件的块则直接返回。否则调用readRecordBlock函数从文件中读取块并返回。
//如果freeSlotOffsets中已无元素，调用newRecordBlock建立一个新块并返回。
//函数自动为块加锁（返回块的外部函数都将自动加锁），并自动设为脏块。
RecordBlock* BufferManager::returnFreeRecordBlock(string tableName) {
	//检查表存在性
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::returnFreeRecordBlock", tableName);
		return nullptr;
	}
	//检查文件是否打开
	string fileName = getFileName(tableName);
	if (!recordFilesOpened.count(fileName)) {
		if (!openRecordFile(tableName)) {
			functionFailureWarning("BufferManager::returnFreeRecordBlock", "BufferManager::openRecordFile");
			return nullptr;
		}
	}
	RecordFile* file = recordFilesOpened.at(fileName);
	RecordBlock* block;
	//仍有freeSlot，选取一个有freeSlot的块返回
	if (!file->freeSlotOffsets.empty()) {
		set<int>::iterator sit;
		for (sit = file->freeSlotOffsets.begin(); sit != file->freeSlotOffsets.end(); sit++) {
			int blockOffset = (*sit - file->FIRST_BLOCK_OFFSET) / BLOCK_SIZE * BLOCK_SIZE + file->FIRST_BLOCK_OFFSET;
			block = file->isBlockInMemory(blockOffset);
			//块在内存中
			if (block != nullptr) {
				if (!block->pinned) {
					block->pinned = true;
					block->isDirty = true;
					return block;
				}
				else continue;
			}
			//块不在内存中，尝试读取
			else {
				block = readRecordBlock(tableName, blockOffset);
				if (block != nullptr) {
					block->pinned = true;
					block->isDirty = true;
					return block;
				}
				else {
					functionFailureWarning("BufferManager::returnFreeRecordBlock", "BufferManager::readRecordBlock");
					continue;
				}
			}
		}
	}
	//没有freeSlot，尝试新建立块
	block = newRecordBlock(tableName);
	if (block != nullptr) {
		block->pinned = true;
		block->isDirty = true;
		return block;
	}
	else {
		functionFailureWarning("BufferManager::returnFreeRecordBlock", "BufferManager::newRecordBlock");
		return nullptr;
	}
}

//函数返回同文件中的下一个块。
//函数先检查块是否在内存中，不在内存中则返回nullptr；
//在内存中，说明表一定存在，且文件已经打开。
//函数计算出需返回块的offset，调用returnRecordBlock返回块。
RecordBlock* BufferManager::returnNextRecordBlock(int curID) {
	//块不在内存中（非法id）
	if (!recordBlocksHeld.count(curID)) {
		invalidIDWarning("BufferManager::returnNextRecordBlock");
		return nullptr;
	}
	RecordBlock* curBlock = recordBlocksHeld.at(curID);
	int nextBlockOffset = curBlock->offset + BLOCK_SIZE;
	RecordFile* file = curBlock->returnFilePtr();
	//当前块是最后一个块
	if (nextBlockOffset >= file->FIRST_BLOCK_OFFSET + file->blockNum * BLOCK_SIZE) return nullptr;

	RecordBlock* block = returnRecordBlock(curBlock->parseTableName(), nextBlockOffset);
	//returnRecordBlock函数会为块加锁，可以直接返回
	if (block) return block;
	else {
		functionFailureWarning("BufferManager::returnNextRecordBlock", "BufferManager::returnRecordBlock");
		return nullptr;
	}
}

//函数返回文件中的第一个块。
//函数先检查表的存在性，其次检查文件是否在内存中。
//当文件已在内存中时，检查其是否有任何块。文件没有块返回nullptr。
//若文件有块，调用returnRecordBlock来返回。
RecordBlock* BufferManager::returnFirstRecordBlock(string tableName) {
	//检查表存在性
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::returnFirstRecordBlock", tableName);
		return nullptr;
	}
	//检查文件是否打开
	string fileName = getFileName(tableName);
	if (!recordFilesOpened.count(fileName)) {
		if (!openRecordFile(tableName)) {
			functionFailureWarning("BufferManager::returnFirstRecordBlock", "BufferManager::openRecordFile");
			return nullptr;
		}
	}
	//文件没有任何块
	RecordFile* file = recordFilesOpened.at(fileName);
	if (file->blockNum == 0) return nullptr;
	//文件有块，调用returnRecordBlock
	else {
		RecordBlock* block = returnRecordBlock(tableName, file->FIRST_BLOCK_OFFSET);
		if (block) return block;
		else {
			functionFailureWarning("BufferManager::returnFirstRecordBlock", "BufferManager::returnRecordBlock");
		}
	}
}

IndexBlock* BufferManager::returnFreeIndexBlock(string tableName, string attriName) {
	//检查属性的存在性
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::returnFreeIndexBlock", tableName, attriName);
		return nullptr;
	}
	//检查文件是否打开
	string fileName = getFileName(tableName, attriName);
	if (!indexFilesOpened.count(fileName)) {
		if (!openIndexFile(tableName, attriName)) {
			functionFailureWarning("BufferManager::returnFreeIndexBlock", "BufferManager::openIndexFile");
			return nullptr;
		}
	}
	//建立新块
	IndexBlock* block = newIndexBlock(tableName, attriName);
	if (block != nullptr) {
		block->pinned = true;
		block->isDirty = true;
		return block;
	}
	else {
		functionFailureWarning("BufferManager::returnFreeIndexBlock", "BufferManager::newIndexBlock");
		return nullptr;
	}
}

//返回索引的根节点。
//函数会检查属性的存在性并打开文件。如果索引没有根（blockNum为0）返回nullptr。
//如有根，向returnIndexBlock传入文件对象保存的rootOffset，返回returnIndexBlock所返回的结果。
IndexBlock* BufferManager::returnRoot(string tableName, string attriName) {
	//检查属性的存在性
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::returnRoot", tableName, attriName);
		return nullptr;
	}
	//检查文件是否打开
	string fileName = getFileName(tableName, attriName);
	if (!indexFilesOpened.count(fileName)) {
		if (!openIndexFile(tableName, attriName)) {
			functionFailureWarning("BufferManager::returnRoot", "BufferManager::openIndexFile");
			return nullptr;
		}
	}
	//文件尚没有块，返回空指针
	IndexFile* file = indexFilesOpened.at(fileName);
	if (file->blockNum == 0) return nullptr;
	//文件有块，调用returnIndexBlock返回根
	else {
		IndexBlock* block = returnIndexBlock(tableName, attriName, file->FIRST_BLOCK_OFFSET);
		if (block) return block;
		else {
			functionFailureWarning("BufferManager::returnRoot", "BufferManager::returnIndexBlock");
			return nullptr;
		}
	}
}

//函数先检查块是否存在，其次检查是否在usedTime链表中。
//若块不存在则直接返回false；若确实在链表中，则将其移至usedTime链表的起始位置；
//不存在于链表中则直接在链表头部插入此id。更新完成后返回true。
bool BufferManager::updateUsedTime(int id) {
	//检查块是否正在使用
	if (!(id <= num_BlocksHeld && (!freeIDs.count(id)))) {
		invalidIDWarning("BufferManager::updateUsedTime");
		return false;
	}
	//遍历usedTime链表，查找此id
	list<int>::iterator lit;
	for (lit = usedTime.begin(); lit != usedTime.end(); lit++) {
		if (*lit == id) {
			usedTime.erase(lit);
			usedTime.push_front(id);
			return true;
		}
	}
	//没有找到此id，直接推到链表前端
	usedTime.push_front(id);
	return true;
}

//函数先检查表是否存在，不存在将返回false。
//若表存在，调用clearFile用有文件头但没有实际数据的文件替换原文件，clearFile同时会释放块和句柄。
//函数将检查clearFile的返回值。删除成功将返回true。
bool BufferManager::dropTable(string tableName) {
	//检查表的存在性
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::dropTable", tableName);
		return false;
	}
	//清除索引和freelist文件
	vector<string> indexedAttriList = returnIndexedAttriList(tableName);
	string indexFileName;
	for (string attriName : indexedAttriList) {
		indexFileName = getFileName(tableName, attriName);
		if (!clearIndexFile(indexFileName)) {
			functionFailureWarning("BufferManager::dropTable", "BufferManager::clearIndexFile");
			return false;
		}
	}
	//清除数据和freelist文件
	string dataFileName = getFileName(tableName);
	if (!clearRecordFile(dataFileName)) {
		functionFailureWarning("BufferManager::dropTable", "BufferManager::clearRecordFile");
		return false;
	}
	return true;
}

//先判断属性的存在性以及是否加了索引，之后调用clearIndexFile来清除文件。
//清除成功返回true，属性不存在、未加索引、清除文件失败返回false。
bool BufferManager::dropIndex(string tableName, string attriName) {
	//检查属性上是否有索引
	if (!isAttriIndexed(tableName, attriName)) {
		attriNotIndexedWarning("BufferManager::dropIndex", tableName, attriName);
		return false;
	}
	string indexFileName = getFileName(tableName, attriName);
	//尝试清除索引文件
	if (!clearIndexFile(indexFileName)) {
		functionFailureWarning("BufferManager::dropIndex", "BufferManager::clearIndexFile");
		return false;
	}
	return true;
}

//函数先检查id的合法性，如id已在内存中，说明块在内存且文件已打开。
//函数遍历向量，每一轮将块解锁后调用forceFreeIndexBlock来释放。
//由于removeIndexBlock会将块解锁，因而forceFreeIndexBlock函数不会失败。
//同时需要调整文件的emptyBlockOffsets和blockNum。
bool BufferManager::removeIndexBlock(vector<int> ids) {
	//检查是否存在非法id
	for (int id : ids) {
		if (!indexBlocksHeld.count(id)) {
			invalidIDWarning("BufferManager::removeIndexBlock");
			return false;
		}
	}
	//逐个释放块
	while (ids.size() > 0) {
		int curID = *(ids.begin());
		IndexBlock* block = indexBlocksHeld.at(curID);
		block->pinned = false;
		//更新文件信息
		IndexFile* file = block->returnFilePtr();
		file->emptyBlockOffsets.insert(block->offset);
		file->blockNum--;
		//释放块
		forceFreeIndexBlock(curID);
		ids.erase(ids.begin());
	}
	return true;
}

//遍历openedIndexFile映射和openedRecordFile映射，调用closeFile来关闭各个文件。
//函数将检查closeFile的返回值，若有文件关闭失败，则Buffer Manager关闭失败，函数将返回false。
//成功关闭所有文件，返回true。
bool BufferManager::shutdown() {
	//关闭数据文件
	while (recordFilesOpened.size() > 0) {
		if (!closeRecordFile(recordFilesOpened.begin()->first)) {
			functionFailureWarning("BufferManager::shutdown", "BufferManager::closeRecordFile");
			cout << "Error: Buffer Manager failed to terminate. Please contact developers for fix solutions." << endl;
			return false;
		}
	}
	//关闭索引文件
	while (indexFilesOpened.size() > 0) {
		if (!closeIndexFile(indexFilesOpened.begin()->first)) {
			functionFailureWarning("BufferManager::shutdown", "BufferManager::closeIndexFile");
			cout << "Error: Buffer Manager failed to terminate. Please contact developers for fix solutions." << endl;
			return false;
		}
	}
	return true;
}