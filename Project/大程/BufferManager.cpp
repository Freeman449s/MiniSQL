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

//��̬��Ա��������
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
	//���������ֵ�
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
	//" - "����Ϊ3��".index"����Ϊ6
	int attriNameLen = fileName.length() - tableNameLen - 3 - 6;
	tableName = fileName.substr(0, tableNameLen);
	attriName = fileName.substr(pos + 2, attriNameLen);
}

void IndexBlock::parseData() {
	//��ȡparent��nextSibling��keyNum
	char* srcp = (char*)srcData;
	memcpy(&parent, srcp, 4);
	srcp += 4;
	memcpy(&nextSibling, srcp, 4);
	srcp += 4;
	memcpy(&keyNum, srcp, 4);
	srcp += 4;
	//��ȡ��������
	string tableName, attriName;
	parseTableAttriName(tableName, attriName);
	const int type = returnIndexType(tableName, attriName);

	//��������
	//����ָ��
	//�������������ռ�
	ptrs = new int[MAX_KEY_NUM + 1];
	int* ip = ptrs;
	for (int i = 1; i <= keyNum + 1; i++) {
		memcpy(ip, srcp, 4);
		ip++;
		srcp += 4;
	}
	//����key
	//����
	if (type == 1) {
		keyi = new int[MAX_KEY_NUM];
		ip = keyi;
		for (int i = 1; i <= keyNum; i++) {
			memcpy(ip, srcp, 4);
			ip++;
			srcp += 4;
		}
	}
	//������
	else if (type == 2) {
		keyf = new float[MAX_KEY_NUM];
		float* flop = keyf;
		for (int i = 1; i <= keyNum; i++) {
			memcpy(flop, srcp, 4);
			flop++;
			srcp += 4;;
		}
	}
	//�ַ�����
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
	//delete��ָ�벻�������쳣
	//delete����ֻ�ͷſռ䣬���Ὣָ�븳��
	srcData = nullptr;
}

void IndexBlock::packData() {
	srcData = new char[8192];
	char* srcp = (char*)srcData;
	//����ֵ���Ϣ
	memcpy(srcp, &parent, 4);
	srcp += 4;
	memcpy(srcp, &nextSibling, 4);
	srcp += 4;
	memcpy(srcp, &keyNum, 4);
	srcp += 4;
	//���ָ��
	for (int i = 0; i <= keyNum; i++) {
		memcpy(srcp, ptrs, 4);
		srcp += 4;
		ptrs++;
	}
	delete[] ptrs;
	ptrs = nullptr;
	//�������
	//����
	if (TYPE == 1) {
		for (int i = 0; i <= keyNum - 1; i++) {
			memcpy(srcp, keyi, 4);
			keyi++;
			srcp += 4;
		}
		delete[] keyi;
		keyi = nullptr;
	}
	//������
	else if (TYPE == 2) {
		for (int i = 0; i <= keyNum - 1; i++) {
			memcpy(srcp, keyf, 4);
			keyf++;
			srcp += 4;
		}
		delete[] keyf;
		keyf = nullptr;
	}
	//�ַ�����
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

//����ʵ�ֳ�Ա������������Ϊstatic�����������޶������������ڱ��ļ��е��ã�����������
bool BufferManager::openRecordFile(string tableName) {
	string fileName = getFileName(tableName);
	//����Ĵ�����
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::openRecordFile", tableName);
		return false;
	}
	//����ļ��Ƿ��Ѵ�
	if (recordFilesOpened.count(fileName)) return true;
	//���Դ������ļ�
	FILE* fp = fopen(fileName.c_str(), "rb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::openRecordFile", fileName);
		return false;
	}
	//��ȡ�ֵ���Ϣ
	int blockNum, recordLen, maxBlockRecordNum;
	fread(&blockNum, 4, 1, fp);
	fread(&recordLen, 4, 1, fp);
	fread(&maxBlockRecordNum, 4, 1, fp);
	fclose(fp);

	RecordFile* file = new RecordFile(tableName, recordLen);
	file->blockNum = blockNum;
	//���Դ�freelist�ļ�
	string freelistFileName = tableName + ".freelist";
	fp = fopen(freelistFileName.c_str(), "rb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::openRecordFile", freelistFileName);
		delete file;
		return false;
	}
	//��ȡfreelist
	while (!feof(fp)) {
		int offset;
		fread(&offset, 4, 1, fp);
		file->freeSlotOffsets.insert(offset);
	}
	fclose(fp);
	//�ļ�����ָ�����recordFilesOpenedӳ��
	recordFilesOpened.insert(pair<string, RecordFile*>(fileName, file));
	return true;
}

bool BufferManager::openIndexFile(string tableName, string attriName) {
	string fileName = getFileName(tableName, attriName);
	//��������Դ�����
	if (!doesAttriExist(tableName, attriName)) {
		string fileName = getFileName(tableName, attriName);
		attriNotExistWarning("BufferManager::openIndexFile", tableName, attriName);
		return false;
	}
	//����ļ��Ƿ��Ѿ���
	if (indexFilesOpened.count(fileName)) return true;
	//���Դ��ļ�
	FILE* fp = fopen(fileName.c_str(), "rb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::openIndexFile", fileName);
		return false;
	}
	//��ȡ�ֵ���Ϣ
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
	//���Զ���freelist��Ϣ
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
	//�ļ�����ָ�����indexFilesOpenedӳ��
	indexFilesOpened.insert(pair<string, IndexFile*>(fileName, file));
	return true;
}

bool BufferManager::createRecordFile(string tableName, int recordLen) {
	string fileName = getFileName(tableName);
	//���������
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::createRecordFile", tableName);
		return false;
	}
	//����ļ��Ƿ��Ѵ�
	if (recordFilesOpened.count(fileName)) return true;
	//����Ƿ��ļ��Ѵ��ڵ�δ��
	else {
		FILE* fp = fopen(fileName.c_str(), "r");
		if (fp != nullptr) {
			fclose(fp);
			return true;
		}
	}
	//����һ���޼�¼�ļ�
	FILE* fp = fopen(fileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::createRecordFile", fileName);
		return false;
	}
	fclose(fp);
	//������freelist�ļ�
	string freelistFileName = getFreelistFileName(fileName);
	fp = fopen(freelistFileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::createRecordFile", freelistFileName);
		return false;
	}
	fclose(fp);
	//�����ļ����󲢼���ӳ��
	RecordFile* file = new RecordFile(tableName, recordLen);
	recordFilesOpened.insert(pair<string, RecordFile*>(fileName, file));
	return true;
}

//Ĭ�ϲ���ֻ�������ں��������뺯����������֮һ
bool BufferManager::createIndexFile(string tableName, string attriName, int type, int keyLen) {
	string fileName = getFileName(tableName, attriName);
	//������Դ�����
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::createIndexFile", tableName, attriName);
		return false;
	}
	//����ļ��Ƿ��Ѵ�
	if (indexFilesOpened.count(fileName)) return true;
	//����Ƿ��ļ�δ�򿪵��Ѵ���
	else {
		FILE* fp = fopen(fileName.c_str(), "r");
		if (fp != nullptr) {
			fclose(fp);
			return true;
		}
	}
	//�����������ļ�
	FILE* fp = fopen(fileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::createIndexFile", fileName);
		return false;
	}
	fclose(fp);
	//������freelist�ļ�
	string freelistFileName = getFreelistFileName(fileName);
	fp = fopen(freelistFileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::createIndexFile", freelistFileName);
		return false;
	}
	fclose(fp);
	//�����ļ����󲢼���ӳ��
	IndexFile* file = new IndexFile(tableName, attriName, type, keyLen);
	indexFilesOpened.insert(pair<string, IndexFile*>(fileName, file));
	return true;
}

//�½�һ�����ݿ顣�����ȼ���Ĵ����Ժ��ļ��Ƿ�򿪣�������ʱ���������Ϣ�����ؿ�ָ�룬�ļ�δ�������openRecordFile��
//����allocateID��ȡһ��ID�������ļ������blockNum��ʼ�����offset���ٳ�ʼ��������Ա��������������������
//�������������recordBlocksHeldӳ�䡣�������������ļ������blockNum��blocksInMemory��freeSlotOffsets��
RecordBlock* BufferManager::newRecordBlock(string tableName) {
	//����Ĵ�����
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::newRecordBlock", tableName);
		return nullptr;
	}
	//����ļ��Ƿ��Ѵ�
	string fileName = getFileName(tableName);
	if (!recordFilesOpened.count(fileName)) {
		if (!openRecordFile(tableName)) {
			functionFailureWarning("BufferManager::newRecordBlock", "BufferManager::openRecordFile");
			return nullptr;
		}
	}
	//�ļ��ɹ��򿪣���������id
	int id = allocateID();
	if (id == -1) {
		functionFailureWarning("BufferManager::newRecordBlock", "BufferManager::allocateID");
		return nullptr;
	}
	//�����¿鲢��ʼ��
	RecordFile* file = recordFilesOpened.at(fileName);
	int offset = file->FIRST_BLOCK_OFFSET + file->blockNum * BLOCK_SIZE;
	int maxRecordNum = file->MAX_BLOCK_RECORD_NUM;
	int recordLen = file->RECORD_LEN;
	RecordBlock* block = new RecordBlock(id, offset, tableName, maxRecordNum, recordLen);
	block->srcData = new char[BLOCK_SIZE];
	block->setAllSlotsAvailable();

	//�����ļ���Ϣ
	file->blockNum++;
	file->blocksInMemory.insert(block);
	int tupleOffset = offset + 4;
	for (int i = 1; i <= maxRecordNum; i++) {
		file->freeSlotOffsets.insert(tupleOffset);
		tupleOffset += 4 + recordLen;
	}

	//�������recordBlocksHeldӳ��
	recordBlocksHeld.insert(pair<int, RecordBlock*>(id, block));
	return block;
}

IndexBlock* BufferManager::newIndexBlock(string tableName, string attriName) {
	//��������ԵĴ�����
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::newIndexBlock", tableName, attriName);
		return nullptr;
	}
	//����ļ��Ƿ��Ѵ�
	string fileName = getFileName(tableName, attriName);
	if (!indexFilesOpened.count(fileName)) {
		if (!openIndexFile(tableName, attriName)) {
			functionFailureWarning("BufferManager::newIndexBlock", "BufferManager::openIndexFile");
			return nullptr;
		}
	}
	//�ļ��ɹ��򿪣���������id
	int id = allocateID();
	if (id == -1) {
		functionFailureWarning("BufferManager::newIndexBlock", "BufferManager::allocateID");
		return nullptr;
	}
	//�����¿鲢��ʼ��
	IndexFile* file = indexFilesOpened.at(fileName);
	int offset;
	//����ʹ���ļ��еĿտ�
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

	//�����ļ���Ϣ
	file->blockNum++;
	file->blocksInMemory.insert(block);

	//�������indexBlocksHeldӳ��
	indexBlocksHeld.insert(pair<int, IndexBlock*>(id, block));
	return block;
}

//����һ�����õĿ�ID. 
//������usedTimeӳ��Ŀ�ͷ����飬�ҵ���һ��δ�����ķ���飬����freeBlock���������ͷš�
//freeBlockʧ����continue. freeBlock�����Ὣ��id����freeIDs��������num_BlocksHeld�Լ���
//���LRU������Ҫ���߼�������Щ�������������usedTime�����޷��ҵ�δ�����ķ���飬
//�����±�������ʱֻѰ��δ�����Ŀ飬����freeBlock�����ͷš�freeBlockʧ����continue��ֱ���ɹ��ͷ�һ���顣
//�����п鶼����������-1. �ú��������ڷ���һ�����õĿ�ID�������ڶ��Ͻ����¿顣
int BufferManager::LRU() {
	list<int>::reverse_iterator rit;
	int id;
	//�����ͷ�һ��δ�����ķ����
	for (rit = usedTime.rbegin(); rit != usedTime.rend(); rit++) {
		id = *rit;
		//���ݿ�
		if (recordBlocksHeld.count(id)) {
			RecordBlock* block = recordBlocksHeld.at(id);
			//δ�����ķ���飬�����ͷ�
			if (!(block->isDirty) && !(block->pinned)) {
				//�ͷųɹ�
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
		//������
		else if (indexBlocksHeld.count(id)) {
			IndexBlock* block = indexBlocksHeld.at(id);
			//δ�����ķ���飬�����ͷ�
			if (!(block->isDirty) && !(block->pinned)) {
				//�ͷųɹ�
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
		//usedTime�г���blocksHeld�в����ڵ�id�����������Ϣ
		else {
			internalErrorWarning("BufferManager::LRU", "Inconsistency between usedTime list and blocksHeld map.");
			continue;
		}
	}
	//�����ͷ�һ��δ�����Ŀ�
	for (rit = usedTime.rbegin(); rit != usedTime.rend(); rit++) {
		id = *rit;
		//���ݿ�
		if (recordBlocksHeld.count(id)) {
			RecordBlock* block = recordBlocksHeld.at(id);
			//δ�����Ŀ飬�����ͷ�
			if (!block->pinned) {
				//�ͷųɹ�
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
		//������
		else if (indexBlocksHeld.count(id)) {
			IndexBlock* block = indexBlocksHeld.at(id);
			//δ�����Ŀ飬�����ͷ�
			if (!block->pinned) {
				//�ͷųɹ�
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
		//usedTime�г���blocksHeld�в����ڵ�id�����������Ϣ
		else {
			internalErrorWarning("BufferManager::LRU", "Inconsistency between usedTime list and blocksHeld map.");
			continue;
		}
	}
	//����8192����ȫ������������-1
	return -1;
}

//���ڷ���һ�����õĿ�ID.�����ȼ��freeIDs�������Ƿ���Ԫ�أ�������ѡȡһ�����أ�
//������Ҫ���num_BlocksHeld�Ƿ��Ѵﵽ���ֵ�����û�У��·���һ��ID�� = num_BlocksHeld + 1��Ȼ�󷵻ء�
//�����������ζ���Ҫ��num_BlocksHeld������
//����Ѿ��ﵽ���ֵ������LRU���ͷ�һ���飬������LRU�ķ���ֵ��
int BufferManager::allocateID() {
	//���Դ�freeIDs��ȡ��һ��id
	if (!freeIDs.empty()) {
		set<int>::iterator sit = freeIDs.begin();
		int ret = *sit;
		freeIDs.erase(sit);
		num_BlocksHeld++;
		return ret;
	}
	//��ǰ���п�����δ�ﵽ���ֵ���·���һ��id
	if (num_BlocksHeld < MAX_BLOCK_NUM) {
		num_BlocksHeld++;
		return num_BlocksHeld;
	}
	//��ǰ���п������Ѿ��ﵽ���ֵ������LRU
	return LRU();
}

//������Ϊ������������������е�recordNum����λ��Ȼ�����ݿ��fileName��offset��Ϣ��
//����fwrite���ļ���д��飬�������ͷſ顣��󣬺�����isDirty����Ϊfalse�����������д��ɹ�������true.
bool BufferManager::writeRecordBlock(int id) {
	RecordBlock* block = recordBlocksHeld.at(id);
	block->pinned = true;
	//������������recordNumλ
	memcpy(block->srcData, &(block->recordNum), 4);
	//���Դ��ļ���д������
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
	//�������
	block->packData();
	//���Դ��ļ���д������
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

//�����ͷ��ڴ��еĿ飬�������飬�����Զ�����д�ء�
//���������Ƿ����ڴ��С��Ƿ�����顢�Ƿ�����������ڴ��еĿ�ֱ�ӷ���true�������Ŀ�ֱ�ӷ���false��
//δ������������writeRecordBlockд�ء����ɹ�д�ػ�����д�أ�δ�����ķ���飩��������id����freeIDs���ϣ�
//��id��usedTime�����recordBlocksHeldӳ���Ƴ�����num_BlocksHeld�Լ����ͷſ������������ȷ�����������ͷţ���
//������ָ����ļ���blocksInMemory�����Ƴ���
bool BufferManager::freeRecordBlock(int id) {
	//����Ĵ����ԡ��Ƿ�������Ƿ���飨����Զ�д�أ�
	if (!recordBlocksHeld.count(id)) return true;
	RecordBlock* block = recordBlocksHeld.at(id);
	if (block->pinned) return false;
	if (block->isDirty) {
		if (!writeRecordBlock(id)) return false;
	}
	//��ʱ�õ���һ����δ�����ķ���飬ִ���ͷŲ���
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
	//����Ĵ����ԡ��Ƿ�������Ƿ���飨����Զ�д�أ�
	if (!indexBlocksHeld.count(id)) return true;
	IndexBlock* block = indexBlocksHeld.at(id);
	if (block->pinned) return false;
	if (block->isDirty) {
		if (!writeIndexBlock(id)) return false;
	}
	//��ʱ�õ���һ����δ�����ķ���飬ִ���ͷŲ���
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

//�������ļ��ж�ȡ�顣
//����������Ƿ���ڡ��ļ��Ƿ�򿪡�����λ�����Ƿ�����п���ڡ����Ƿ������ڴ��С������߱�֤offsetǡΪ�����ʼ��ַ��
//����allocateID�õ�һ�����õ�ID���½�һ������ΪID��ֵ��������recordBlocksHeldӳ�䡣
//����Ϊoffset��fileName��MAX_RECORD_NUM��ֵ��Ϊdata����ռ䲢�������ݣ���ȡrecordNum�����ำĬ��ֵ��
//�������轫���ָ������ļ������blocksInMemory������
RecordBlock* BufferManager::readRecordBlock(string tableName, int offset) {
	//����Ĵ�����
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::readRecordBlock", tableName);
		return nullptr;
	}
	//����ļ��Ƿ��Ѿ���
	string fileName = getFileName(tableName);
	if (!recordFilesOpened.count(fileName)) {
		if (!openRecordFile(tableName)) {
			functionFailureWarning("BufferManager::readRecordBlock", "BufferManager::openRecordFile");
			return nullptr;
		}
	}
	//���ƫ�����ĺϷ���
	RecordFile* file = recordFilesOpened.at(fileName);
	if (offset >= file->FIRST_BLOCK_OFFSET + file->blockNum * BLOCK_SIZE || offset < file->FIRST_BLOCK_OFFSET) {
		offsetOutOfRangeWarning("BufferManager::readRecordBlock");
		return nullptr;
	}
	//�����Ƿ������ڴ���
	set<RecordBlock*>::iterator sit;
	for (sit = file->blocksInMemory.begin(); sit != file->blocksInMemory.end(); sit++) {
		if ((*sit)->offset == offset) return *sit;
	}
	//����ID
	int id = allocateID();
	if (id == -1) {
		functionFailureWarning("BufferManager::readRecordBlock", "BufferManager::allocateID");
		return nullptr;
	}
	//�ɹ�����ID���½������
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
	//�����ļ�
	file->blocksInMemory.insert(block);
	return block;
}

IndexBlock* BufferManager::readIndexBlock(string tableName, string attriName, int offset) {
	//������Դ�����
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::readIndexBlock", tableName, attriName);
		return nullptr;
	}
	//����ļ��Ƿ��Ѿ���
	string fileName = getFileName(tableName, attriName);
	if (!indexFilesOpened.count(fileName)) {
		if (!openIndexFile(tableName, attriName)) {
			functionFailureWarning("BufferManager::readIndexBlock", "BufferManager::openIndexFile");
			return nullptr;
		}
	}
	//���ƫ�����ĺϷ���
	IndexFile* file = indexFilesOpened.at(fileName);
	if (!file->isBlockOffsetValid(offset)) {
		offsetOutOfRangeWarning("BufferManager::readIndexBlock");
		return nullptr;
	}
	//�����Ƿ������ڴ���
	set<IndexBlock*>::iterator sit;
	for (sit = file->blocksInMemory.begin(); sit != file->blocksInMemory.end(); sit++) {
		if ((*sit)->offset == offset) return *sit;
	}
	//����ID
	int id = allocateID();
	if (id == -1) {
		functionFailureWarning("BufferManager::readIndexBlock", "BufferManager::allocateID");
		return nullptr;
	}
	//�ɹ�����ID���½������
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
	//�����ļ�
	file->blocksInMemory.insert(block);
	return block;
}

//��clearRecordFile���ã��ú�����֤�����ڴ��С�
//���ӿ��Ƿ�����飬ǿ�ƽ����ͷţ�����Ȼ�ܵ������ơ�
//�������ȼ����Ƿ�����������Ŀ�ֱ�ӷ���false��
//�������ͨ�����ͷ�srcData����������id��recordBlocksHeldӳ���Ƴ�����num_BlocksHeld�Լ�����usedTime�����Ƴ�id��
//��id����freeIDs���ϣ�������ָ����ļ������blocksInMemory�����Ƴ����ɹ��ͷź󷵻�true��
bool BufferManager::forceFreeRecordBlock(int id) {
	RecordBlock* block = recordBlocksHeld.at(id);
	if (block->pinned) {
		blockPinnedWarning("BufferManager::forceFreeRecordBlock", block->fileName);
		return false;
	}

	//��BufferManager��������������Ϣ
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

	//�����ļ�
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

	//��BufferManager��������������Ϣ
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

	//�����ļ�
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

//��dropTable���ã��ú�����֤�ļ����ڡ�
//�ȵ���forceFreeRecordBlock����ͷ��ļ����ڴ������ֵĿ飬�ļ������ڴ���ʱ�����˲���
//�������forceFreeRecordBlock�ķ���ֵ�������ĳ�ε��÷���false�����ļ�ɾ��ʧ�ܣ���������false��
//֮�������ļ�ͷ��û��ʵ�����ݵ��ļ��滻ԭ�����ļ�������ļ������openedRecordFilesӳ�����Ƴ����ͷ��ļ����������Ѵ򿪣���
bool BufferManager::clearRecordFile(string fileName) {
	//�ȼ���ļ��Ƿ����ڴ���
	if (recordFilesOpened.count(fileName)) {
		//����ͷ��ڴ��еĿ�
		RecordFile* file = recordFilesOpened.at(fileName);
		while (!file->blocksInMemory.empty()) {
			//����forceFreeRecordBlock���blocksInMemory�������Ƴ�ָ�룬�����ظ��Ƴ�
			if (forceFreeRecordBlock((*(file->blocksInMemory.begin()))->id)) continue;
			else {
				functionFailureWarning("BufferManager::clearRecordFile", "BufferManager::forceFreeRecordBlock");
				return false;
			}
		}
	}
	//����ļ�����
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
	//�Ƴ��ļ������ͷž��
	if (recordFilesOpened.count(fileName)) {
		RecordFile* file = recordFilesOpened.at(fileName);
		map<string, RecordFile*>::iterator mit = recordFilesOpened.find(fileName);
		recordFilesOpened.erase(mit);
		delete file;
	}

	return true;
}

bool BufferManager::clearIndexFile(string fileName) {
	//�ȼ���ļ��Ƿ����ڴ���
	if (indexFilesOpened.count(fileName)) {
		//����ͷ��ڴ��еĿ�
		IndexFile* file = indexFilesOpened.at(fileName);
		while (!file->blocksInMemory.empty()) {
			//����forceFreeIndexBlock���blocksInMemory�������Ƴ�ָ�룬�����ظ��Ƴ�
			if (forceFreeIndexBlock((*(file->blocksInMemory.begin()))->id)) continue;
			else {
				functionFailureWarning("BufferManager::clearIndexFile", "BufferManager::forceFreeIndexBlock");
				return false;
			}
		}
	}
	//����ļ�����
	FILE* fp = fopen(fileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::clearIndexFile", fileName);
		return false;
	}
	fclose(fp);
	//���freelist
	string freelistFileName = getFreelistFileName(fileName);
	fp = fopen(freelistFileName.c_str(), "wb");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::clearIndexFile", freelistFileName);
		return false;
	}
	fclose(fp);
	//�Ƴ��ļ�����
	if (indexFilesOpened.count(fileName)) {
		IndexFile* file = indexFilesOpened.at(fileName);
		map<string, IndexFile*>::iterator mit = indexFilesOpened.find(fileName);
		indexFilesOpened.erase(mit);
		delete file;
	}

	return true;
}

//��shutdown�������ã��ú�����֤�����ļ��ľ��һ���Ѿ����ڡ�
//����blocksInMemory���������鵱ǰ�ļ����ڴ��г��еĿ飬��freeRecordBlock����ͷţ�
//�����п��ͷ�ʧ��ʱ���ļ��ر�ʧ�ܣ���������false����
//֮�������ļ�д���ļ�ͷ��һЩ��Ϣ��������һ���������ļ�д��freelist��Ϣ��
//������������openedRecordFilesӳ���Ƴ����ͷ��ļ������
bool BufferManager::closeRecordFile(string fileName) {
	RecordFile* file = recordFilesOpened.at(fileName);
	//�ͷſ�
	while (!file->blocksInMemory.empty()) {
		if (freeRecordBlock((*(file->blocksInMemory.begin()))->id)) continue;
		else {
			functionFailureWarning("BufferManager::closeRecordFile", "BufferManager::freeRecordBlock");
			return false;
		}
	}
	//�����ļ�ͷ
	FILE* fp = fopen(fileName.c_str(), "rb+");
	if (fp == nullptr) {
		fileCannotOpenWarning("BufferManager::closeRecordFile", fileName);
		return false;
	}
	fwrite(&(file->blockNum), 4, 1, fp);
	//�̶�ֵҲ����дһ�飬ȷ���½����ļ����ļ�ͷ�ܹ���д�����
	fwrite(&(file->RECORD_LEN), 4, 1, fp);
	fwrite(&(file->MAX_BLOCK_RECORD_NUM), 4, 1, fp);
	fclose(fp);
	//����freelist�ļ���
	string freelistFileName = getFreelistFileName(fileName);
	//д��freelist
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
	//�Ƴ��ļ�����ָ�벢�ͷ��ļ�����
	map<string, RecordFile*>::iterator mit = recordFilesOpened.find(fileName);
	recordFilesOpened.erase(mit);
	delete file;

	return true;
}

bool BufferManager::closeIndexFile(string fileName) {
	IndexFile* file = indexFilesOpened.at(fileName);
	//�ͷſ�
	while (!file->blocksInMemory.empty()) {
		if (freeIndexBlock((*(file->blocksInMemory.begin()))->id)) continue;
		else {
			functionFailureWarning("BufferManager::closeIndexFile", "BufferManager::freeIndexBlock");
			return false;
		}
	}
	//�����ļ�ͷ
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
	//дfreelist
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
	//�Ƴ��ļ�����ָ�벢�ͷ��ļ�����
	map<string, IndexFile*>::iterator mit = indexFilesOpened.find(fileName);
	indexFilesOpened.erase(mit);
	delete file;

	return true;
}

//�������ݴ���ı�����offset���ؿ顣
//������Ҫ��offsetǡΪ�����ʼƫ���������ǿ������ݴ���ƫ����������������ƫ�������ٽ���ȡ�ء�
//�����ȼ���Ĵ����ԣ��ټ���ļ��Ƿ��Ѵ򿪣�֮���鴫���λ�����Ƿ�ȷʵ�п���ڣ�����blockNum����
//֮�󣬺��������Ƿ������ڴ��У���������ֱ�ӷ��أ��������readRecordBlock���ļ���ȡ��
//�����Զ�Ϊ�������
RecordBlock* BufferManager::returnRecordBlock(string tableName, int offset) {
	//����Ĵ�����
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::returnRecordBlock", tableName);
		return nullptr;
	}
	//����ļ��Ƿ��
	string fileName = getFileName(tableName);
	if (!recordFilesOpened.count(fileName)) {
		if (!openRecordFile(tableName)) {
			functionFailureWarning("BufferManager::returnRecordBlock", "BufferManager::openRecordFile");
			return nullptr;
		}
	}
	//�������ƫ����
	RecordFile* file = recordFilesOpened.at(fileName);
	offset = (offset - file->FIRST_BLOCK_OFFSET) / BLOCK_SIZE * BLOCK_SIZE + file->FIRST_BLOCK_OFFSET;
	//���ƫ�����Ϸ���
	if (offset < file->FIRST_BLOCK_OFFSET || offset >= file->FIRST_BLOCK_OFFSET + file->blockNum * BLOCK_SIZE) {
		offsetOutOfRangeWarning("BufferManager::returnRecordBlock");
		return nullptr;
	}
	//���Դ��ڴ��ж�ȡ��
	RecordBlock* block = file->isBlockInMemory(offset);
	if (block != nullptr) return block;
	//���ļ���ȡ��
	block = readRecordBlock(tableName, offset);
	if (block == nullptr) {
		functionFailureWarning("BufferManager::returnRecordBlock", "BufferManager::readRecordBlock");
		return nullptr;
	}
	//�Զ�����
	block->pinned = true;
	return block;
}

IndexBlock* BufferManager::returnIndexBlock(string tableName, string attriName, int offset) {
	//������ԵĴ�����
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::returnIndexBlock", tableName, attriName);
		return nullptr;
	}
	//����ļ��Ƿ��
	string fileName = getFileName(tableName, attriName);
	if (!indexFilesOpened.count(fileName)) {
		if (!openIndexFile(tableName, attriName)) {
			functionFailureWarning("BufferManager::returnIndexBlock", "BufferManager::openIndexFile");
			return nullptr;
		}
	}
	//���ƫ�����Ϸ���
	IndexFile* file = indexFilesOpened.at(fileName);
	if (!file->isBlockOffsetValid(offset)) {
		offsetOutOfRangeWarning("BufferManager::returnIndexBlock");
		return nullptr;
	}
	//���Դ��ڴ��ж�ȡ��
	IndexBlock* block = file->isBlockInMemory(offset);
	if (block != nullptr) return block;
	//���ļ���ȡ��
	block = readIndexBlock(tableName, attriName, offset);
	if (block == nullptr) {
		functionFailureWarning("BufferManager::returnIndexBlock", "BufferManager::readIndexBlock");
		return nullptr;
	}
	//�Զ�����
	block->pinned = true;
	return block;
}

//��������һ�����п顣
//�����ȼ���Ĵ����ԡ���μ���¼�ļ��Ƿ��Ѵ򿪣���δ�򿪵���openRecordFile���ļ���
//����openedRecordFilesӳ�䶨λRecordFile���󣬼��freeSlotOffsets���Ƿ���Ԫ�ء�
//���У�������blocksInMemory�����еĸ����飬���Ƿ��п��offset���ϣ��Լ��ÿ��Ƿ�������
//���з��������Ŀ���ֱ�ӷ��ء��������readRecordBlock�������ļ��ж�ȡ�鲢���ء�
//���freeSlotOffsets������Ԫ�أ�����newRecordBlock����һ���¿鲢���ء�
//�����Զ�Ϊ����������ؿ���ⲿ���������Զ������������Զ���Ϊ��顣
RecordBlock* BufferManager::returnFreeRecordBlock(string tableName) {
	//���������
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::returnFreeRecordBlock", tableName);
		return nullptr;
	}
	//����ļ��Ƿ��
	string fileName = getFileName(tableName);
	if (!recordFilesOpened.count(fileName)) {
		if (!openRecordFile(tableName)) {
			functionFailureWarning("BufferManager::returnFreeRecordBlock", "BufferManager::openRecordFile");
			return nullptr;
		}
	}
	RecordFile* file = recordFilesOpened.at(fileName);
	RecordBlock* block;
	//����freeSlot��ѡȡһ����freeSlot�Ŀ鷵��
	if (!file->freeSlotOffsets.empty()) {
		set<int>::iterator sit;
		for (sit = file->freeSlotOffsets.begin(); sit != file->freeSlotOffsets.end(); sit++) {
			int blockOffset = (*sit - file->FIRST_BLOCK_OFFSET) / BLOCK_SIZE * BLOCK_SIZE + file->FIRST_BLOCK_OFFSET;
			block = file->isBlockInMemory(blockOffset);
			//�����ڴ���
			if (block != nullptr) {
				if (!block->pinned) {
					block->pinned = true;
					block->isDirty = true;
					return block;
				}
				else continue;
			}
			//�鲻���ڴ��У����Զ�ȡ
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
	//û��freeSlot�������½�����
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

//��������ͬ�ļ��е���һ���顣
//�����ȼ����Ƿ����ڴ��У������ڴ����򷵻�nullptr��
//���ڴ��У�˵����һ�����ڣ����ļ��Ѿ��򿪡�
//����������践�ؿ��offset������returnRecordBlock���ؿ顣
RecordBlock* BufferManager::returnNextRecordBlock(int curID) {
	//�鲻���ڴ��У��Ƿ�id��
	if (!recordBlocksHeld.count(curID)) {
		invalidIDWarning("BufferManager::returnNextRecordBlock");
		return nullptr;
	}
	RecordBlock* curBlock = recordBlocksHeld.at(curID);
	int nextBlockOffset = curBlock->offset + BLOCK_SIZE;
	RecordFile* file = curBlock->returnFilePtr();
	//��ǰ�������һ����
	if (nextBlockOffset >= file->FIRST_BLOCK_OFFSET + file->blockNum * BLOCK_SIZE) return nullptr;

	RecordBlock* block = returnRecordBlock(curBlock->parseTableName(), nextBlockOffset);
	//returnRecordBlock������Ϊ�����������ֱ�ӷ���
	if (block) return block;
	else {
		functionFailureWarning("BufferManager::returnNextRecordBlock", "BufferManager::returnRecordBlock");
		return nullptr;
	}
}

//���������ļ��еĵ�һ���顣
//�����ȼ���Ĵ����ԣ���μ���ļ��Ƿ����ڴ��С�
//���ļ������ڴ���ʱ��������Ƿ����κο顣�ļ�û�п鷵��nullptr��
//���ļ��п飬����returnRecordBlock�����ء�
RecordBlock* BufferManager::returnFirstRecordBlock(string tableName) {
	//���������
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::returnFirstRecordBlock", tableName);
		return nullptr;
	}
	//����ļ��Ƿ��
	string fileName = getFileName(tableName);
	if (!recordFilesOpened.count(fileName)) {
		if (!openRecordFile(tableName)) {
			functionFailureWarning("BufferManager::returnFirstRecordBlock", "BufferManager::openRecordFile");
			return nullptr;
		}
	}
	//�ļ�û���κο�
	RecordFile* file = recordFilesOpened.at(fileName);
	if (file->blockNum == 0) return nullptr;
	//�ļ��п飬����returnRecordBlock
	else {
		RecordBlock* block = returnRecordBlock(tableName, file->FIRST_BLOCK_OFFSET);
		if (block) return block;
		else {
			functionFailureWarning("BufferManager::returnFirstRecordBlock", "BufferManager::returnRecordBlock");
		}
	}
}

IndexBlock* BufferManager::returnFreeIndexBlock(string tableName, string attriName) {
	//������ԵĴ�����
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::returnFreeIndexBlock", tableName, attriName);
		return nullptr;
	}
	//����ļ��Ƿ��
	string fileName = getFileName(tableName, attriName);
	if (!indexFilesOpened.count(fileName)) {
		if (!openIndexFile(tableName, attriName)) {
			functionFailureWarning("BufferManager::returnFreeIndexBlock", "BufferManager::openIndexFile");
			return nullptr;
		}
	}
	//�����¿�
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

//���������ĸ��ڵ㡣
//�����������ԵĴ����Բ����ļ����������û�и���blockNumΪ0������nullptr��
//���и�����returnIndexBlock�����ļ����󱣴��rootOffset������returnIndexBlock�����صĽ����
IndexBlock* BufferManager::returnRoot(string tableName, string attriName) {
	//������ԵĴ�����
	if (!doesAttriExist(tableName, attriName)) {
		attriNotExistWarning("BufferManager::returnRoot", tableName, attriName);
		return nullptr;
	}
	//����ļ��Ƿ��
	string fileName = getFileName(tableName, attriName);
	if (!indexFilesOpened.count(fileName)) {
		if (!openIndexFile(tableName, attriName)) {
			functionFailureWarning("BufferManager::returnRoot", "BufferManager::openIndexFile");
			return nullptr;
		}
	}
	//�ļ���û�п飬���ؿ�ָ��
	IndexFile* file = indexFilesOpened.at(fileName);
	if (file->blockNum == 0) return nullptr;
	//�ļ��п飬����returnIndexBlock���ظ�
	else {
		IndexBlock* block = returnIndexBlock(tableName, attriName, file->FIRST_BLOCK_OFFSET);
		if (block) return block;
		else {
			functionFailureWarning("BufferManager::returnRoot", "BufferManager::returnIndexBlock");
			return nullptr;
		}
	}
}

//�����ȼ����Ƿ���ڣ���μ���Ƿ���usedTime�����С�
//���鲻������ֱ�ӷ���false����ȷʵ�������У���������usedTime�������ʼλ�ã�
//����������������ֱ��������ͷ�������id��������ɺ󷵻�true��
bool BufferManager::updateUsedTime(int id) {
	//�����Ƿ�����ʹ��
	if (!(id <= num_BlocksHeld && (!freeIDs.count(id)))) {
		invalidIDWarning("BufferManager::updateUsedTime");
		return false;
	}
	//����usedTime�������Ҵ�id
	list<int>::iterator lit;
	for (lit = usedTime.begin(); lit != usedTime.end(); lit++) {
		if (*lit == id) {
			usedTime.erase(lit);
			usedTime.push_front(id);
			return true;
		}
	}
	//û���ҵ���id��ֱ���Ƶ�����ǰ��
	usedTime.push_front(id);
	return true;
}

//�����ȼ����Ƿ���ڣ������ڽ�����false��
//������ڣ�����clearFile�����ļ�ͷ��û��ʵ�����ݵ��ļ��滻ԭ�ļ���clearFileͬʱ���ͷſ�;����
//���������clearFile�ķ���ֵ��ɾ���ɹ�������true��
bool BufferManager::dropTable(string tableName) {
	//����Ĵ�����
	if (!doesTableExist(tableName)) {
		tableNotExistWarning("BufferManager::dropTable", tableName);
		return false;
	}
	//���������freelist�ļ�
	vector<string> indexedAttriList = returnIndexedAttriList(tableName);
	string indexFileName;
	for (string attriName : indexedAttriList) {
		indexFileName = getFileName(tableName, attriName);
		if (!clearIndexFile(indexFileName)) {
			functionFailureWarning("BufferManager::dropTable", "BufferManager::clearIndexFile");
			return false;
		}
	}
	//������ݺ�freelist�ļ�
	string dataFileName = getFileName(tableName);
	if (!clearRecordFile(dataFileName)) {
		functionFailureWarning("BufferManager::dropTable", "BufferManager::clearRecordFile");
		return false;
	}
	return true;
}

//���ж����ԵĴ������Լ��Ƿ����������֮�����clearIndexFile������ļ���
//����ɹ�����true�����Բ����ڡ�δ������������ļ�ʧ�ܷ���false��
bool BufferManager::dropIndex(string tableName, string attriName) {
	//����������Ƿ�������
	if (!isAttriIndexed(tableName, attriName)) {
		attriNotIndexedWarning("BufferManager::dropIndex", tableName, attriName);
		return false;
	}
	string indexFileName = getFileName(tableName, attriName);
	//������������ļ�
	if (!clearIndexFile(indexFileName)) {
		functionFailureWarning("BufferManager::dropIndex", "BufferManager::clearIndexFile");
		return false;
	}
	return true;
}

//�����ȼ��id�ĺϷ��ԣ���id�����ڴ��У�˵�������ڴ����ļ��Ѵ򿪡�
//��������������ÿһ�ֽ�����������forceFreeIndexBlock���ͷš�
//����removeIndexBlock�Ὣ����������forceFreeIndexBlock��������ʧ�ܡ�
//ͬʱ��Ҫ�����ļ���emptyBlockOffsets��blockNum��
bool BufferManager::removeIndexBlock(vector<int> ids) {
	//����Ƿ���ڷǷ�id
	for (int id : ids) {
		if (!indexBlocksHeld.count(id)) {
			invalidIDWarning("BufferManager::removeIndexBlock");
			return false;
		}
	}
	//����ͷſ�
	while (ids.size() > 0) {
		int curID = *(ids.begin());
		IndexBlock* block = indexBlocksHeld.at(curID);
		block->pinned = false;
		//�����ļ���Ϣ
		IndexFile* file = block->returnFilePtr();
		file->emptyBlockOffsets.insert(block->offset);
		file->blockNum--;
		//�ͷſ�
		forceFreeIndexBlock(curID);
		ids.erase(ids.begin());
	}
	return true;
}

//����openedIndexFileӳ���openedRecordFileӳ�䣬����closeFile���رո����ļ���
//���������closeFile�ķ���ֵ�������ļ��ر�ʧ�ܣ���Buffer Manager�ر�ʧ�ܣ�����������false��
//�ɹ��ر������ļ�������true��
bool BufferManager::shutdown() {
	//�ر������ļ�
	while (recordFilesOpened.size() > 0) {
		if (!closeRecordFile(recordFilesOpened.begin()->first)) {
			functionFailureWarning("BufferManager::shutdown", "BufferManager::closeRecordFile");
			cout << "Error: Buffer Manager failed to terminate. Please contact developers for fix solutions." << endl;
			return false;
		}
	}
	//�ر������ļ�
	while (indexFilesOpened.size() > 0) {
		if (!closeIndexFile(indexFilesOpened.begin()->first)) {
			functionFailureWarning("BufferManager::shutdown", "BufferManager::closeIndexFile");
			cout << "Error: Buffer Manager failed to terminate. Please contact developers for fix solutions." << endl;
			return false;
		}
	}
	return true;
}