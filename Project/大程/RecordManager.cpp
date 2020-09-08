// (C) 2020, Yuxuan Zhou
// RecordManager.cpp
// All rights reserved.

#include"RecordManager.h"

bool insert_record(string str)
{
	int tupleOffset = 4;
	vector<string> tuple = stringSplit(str);
	if (isUnique(tuple)) return false;
	string table_name = tuple[0];
	int str_len = tuple.size();
	RecordBlock* in_record = BufferManager::returnFreeRecordBlock(table_name);
	int recordLen = returnRecordLen(table_name);
	int n = 1;
	char* datap = (char*)in_record->srcData;
	datap += tupleOffset;
	int isDelete;
	memcpy(&isDelete, datap, 4);
	while (isDelete == 1)
	{
		n++;
		datap += recordLen + 4;
		memcpy(&isDelete, datap, 4);
	}
	int flag = 1;
	memcpy(datap, &flag, 4);
	in_record->returnFilePtr()->removeFreeSlotOffset(datap-in_record->srcData+in_record->returnOffset());
	recordNumIncrease(table_name);
	vector<int> AttrLengths = returnAttrLengths(table_name);
	vector<string> AttrType = returnAttriType(table_name);
	int i = 0, j = 1;
	datap += 4;
	while (j < str_len - 1)
	{
		if (AttrType[i] == "int")
		{
			int value = atoi(tuple[j].c_str());
			memcpy(datap, &value, AttrLengths[i]);
		}
		else if (AttrType[i] == "float")
		{
			float value = atof(tuple[j].c_str());
			memcpy(datap, &value, AttrLengths[i]);
		}
		else if (AttrType[i] == "char")
		{
			tuple[j] = tuple[j].substr(1, tuple[j].length() - 2);
			int length = tuple[j].length();
			memcpy(datap, tuple[j].c_str(), AttrLengths[i]);
			if (tuple[j].length() >= AttrLengths[i])
			{
				datap[AttrLengths[i] - 1] = '\0';
			}

		}
		datap += AttrLengths[i];
		i++;
		j++;
	}
	in_record->recordNum++;
	in_record->setDirty();
	in_record->unpin();
	BufferManager::updateUsedTime(in_record->returnID());
	return true;
}

void select_record(string str)
{
	int tupleOffset = 4;
	vector<string> input = stringSplit(str);
	string table_name = input[0];
	print_AttrName(table_name);
	int str_len = input.size();
	RecordBlock* se_record = BufferManager::returnFirstRecordBlock(table_name);
	int recordLen = returnRecordLen(table_name);
	int n = 1;
	char* datap = (char*)se_record->srcData;
	datap += tupleOffset;
	int isDelete;
	memcpy(&isDelete, datap, 4);
	if (str_len == 2)
	{
		while (datap - (char*)se_record->srcData <= 8192)
		{
			if (isDelete == 1) print_tuple(datap,table_name);
			datap += recordLen + 4;
			memcpy(&isDelete, datap, 4);
		}
	}
	else if (str_len == 4)
	{
		vector<string> Attr_Name = returnAttriNames(table_name);
		vector<int> AttrLengths = returnAttrLengths(table_name);
		vector<string> Attr_Type = returnAttriType(table_name);
		while (datap - (char*)se_record->srcData <= 8192)
		{
			if (isDelete == 1)
			{
				if (isWhereOne(datap,input, AttrLengths,Attr_Name,Attr_Type) == true) print_tuple(datap, table_name);
				datap += recordLen + 4;
				memcpy(&isDelete, datap, 4);
			}
			else datap += recordLen + 4;
		}

	}

	else if (str_len == 9)
	{
		vector<string> Attr_Name = returnAttriNames(table_name);
		vector<int> AttrLengths = returnAttrLengths(table_name);
		vector<string> Attr_Type = returnAttriType(table_name);
		while (datap - (char*)se_record->srcData <= 8192)
		{
			if (isDelete == 1)
			{
				if (isWhereTwo(datap, input, AttrLengths, Attr_Name, Attr_Type)) print_tuple(datap, table_name);
				datap += recordLen + 4;
				memcpy(&isDelete, datap, 4);
			}
			else datap += recordLen + 4;
		}
	}
	RecordBlock* se_ne_record = BufferManager::returnNextRecordBlock(se_record->returnID());
	se_record->unpin();
	while (se_ne_record != nullptr)
	{
		int n_ne = 1;
		char* datap_ne = (char*)se_ne_record->srcData;
		datap_ne += tupleOffset;
		int isDelete_ne;
		memcpy(&isDelete_ne, datap_ne, 4);
		if (str_len == 2)
		{
			while (datap_ne - (char*)se_ne_record->srcData <= 8192)
			{
				if (isDelete_ne == 1) print_tuple(datap_ne, table_name);
				datap_ne += recordLen + 4;
				memcpy(&isDelete_ne, datap_ne, 4);
			}
		}
		else if (str_len == 4)
		{
			vector<string> Attr_Name_ne = returnAttriNames(table_name);
			vector<int> AttrLengths_ne = returnAttrLengths(table_name);
			vector<string> Attr_Type_ne = returnAttriType(table_name);
			while (datap_ne - (char*)se_ne_record->srcData <= 8192)
			{
				if (isDelete_ne == 1)
				{
					if (isWhereOne(datap_ne, input, AttrLengths_ne, Attr_Name_ne, Attr_Type_ne) == true) print_tuple(datap_ne, table_name);
					datap_ne += recordLen + 4;
					memcpy(&isDelete_ne, datap_ne, 4);
				}
				else datap_ne += recordLen + 4;
			}

		}

		else if (str_len == 9)
		{
			vector<string> Attr_Name_ne = returnAttriNames(table_name);
			vector<int> AttrLengths_ne = returnAttrLengths(table_name);
			vector<string> Attr_Type_ne = returnAttriType(table_name);
			while (datap - (char*)se_record->srcData <= 8192)
			{
				if (isDelete_ne == 1)
				{
					if (isWhereTwo(datap_ne, input, AttrLengths_ne, Attr_Name_ne, Attr_Type_ne)) print_tuple(datap, table_name);
					datap_ne += recordLen + 4;
					memcpy(&isDelete_ne, datap_ne, 4);
				}
				else datap_ne += recordLen + 4;
			}
		}
		se_ne_record->unpin();
		se_ne_record = BufferManager::returnNextRecordBlock(se_ne_record->returnID());
	}
}

bool isUnique(vector<string> tuple)
{
	string table_name = tuple[0];
	RecordBlock* test_record = BufferManager::returnFirstRecordBlock(table_name);
	if (test_record  == nullptr) return false;
	int num = tuple.size();
	vector<string> AttrName = returnAttriNames(table_name);
	vector<string> AttrType = returnAttriType(table_name);
	vector<int>AttrLength = returnAttrLengths(table_name);
	int i = 0;
	for (;i < num - 2;i++)
	{
		if (return_if_unique(table_name, AttrName[i]) == false) continue;
		RecordBlock* un_record = BufferManager::returnFirstRecordBlock(table_name);
		int recordLen = returnRecordLen(table_name);

		char* datap = (char*)un_record->srcData;
		datap += 4;
		int isDelete;
		memcpy(&isDelete, datap, 4);
		datap += 4;
		while (datap - (char*)un_record->srcData <= 8192)
		{
			if (isDelete == 1)
			{
				for (int j = 0;j < i;j++) datap += AttrLength[j];
				if (AttrType[i] == "int")
				{
					int* value = new int;
					memcpy(value, datap, 4);
					int comp = atoi(tuple[i + 1].c_str());
					if (*value == comp) return true;
				}
				else if (AttrType[i] == "float")
				{
					float* value = new float;
					memcpy(value, datap, 4);
					float comp = atof(tuple[i + 1].c_str());
					if (*value == comp) return true;
				}
				else if (AttrType[i] == "char")
				{
					char* value = new char[AttrLength[i]];
					memcpy(value, datap, AttrLength[i]);
					string co_str = tuple[i + 1].substr(1, tuple[i + 1].length() - 2);
					string va_str = value;
					if (va_str == co_str) return true;
				}
				for (int j = 0;j < i;j++) datap -= AttrLength[j];
			}
			datap += recordLen + 4;
			memcpy(&isDelete, datap, 4);
		}
		RecordBlock* un_ne_record = BufferManager::returnNextRecordBlock(un_record->returnID());
		un_record->unpin();
		while (un_ne_record != nullptr)
		{
			char* datap_ne = (char*)un_ne_record->srcData;
			datap_ne += 4;
			int isDelete_ne;
			memcpy(&isDelete_ne, datap_ne, 4);
			while (datap_ne - (char*)un_ne_record->srcData <= 8192)
			{
				if (isDelete_ne == 1)
				{
					for (int j = 0;j < i;j++) datap_ne += AttrLength[j];
					if (AttrType[i] == "int")
					{
						int* value_ne = new int;
						memcpy(value_ne, datap_ne, 4);
						int comp_ne = atoi(tuple[i + 1].c_str());
						if (*value_ne == comp_ne) return true;
					}
					else if (AttrType[i] == "float")
					{
						float* value_ne = new float;
						memcpy(value_ne, datap_ne, 4);
						float comp_ne = atof(tuple[i + 1].c_str());
						if (*value_ne == comp_ne) return true;
					}
					else if (AttrType[i] == "char")
					{
						char* value_ne = new char[AttrLength[i]];
						memcpy(value_ne, datap, AttrLength[i]);
						string co_ne_str = tuple[i + 1].substr(1, tuple[i + 1].length() - 2);
						string va_ne_str = value_ne;
						if (va_ne_str == co_ne_str) return true;
					}
					for (int j = 0;j < i;j++) datap -= AttrLength[j];
				}
				datap_ne += recordLen + 4;
				memcpy(&isDelete, datap, 4);
			}
			un_record->unpin();
			RecordBlock* un_ne_record = BufferManager::returnNextRecordBlock(un_ne_record->returnID());
		}
	}
	return false;
}
void print_AttrName(string table_name)
{
	vector<string> Attr_Name = returnAttriNames(table_name);
	for (int i = 0;i < Attr_Name.size();i++)
	{
		cout << Attr_Name[i] << " ";
	}
	cout << endl;
}

void print_tuple(char* datap, string table_name)
{
	datap += 4;
	vector<int> AttrLengths = returnAttrLengths(table_name);
	vector<string> AttrType = returnAttriType(table_name);
	int Attr_len = AttrLengths.size();
	for (int i = 0;i < Attr_len;datap += AttrLengths[i],i++)
	{
		if (AttrType[i] == "int")
		{
			int tmp;
			memcpy(&tmp, datap, 4);
			cout << tmp << " ";
		}
		else if (AttrType[i] == "float")
		{
			float tmp;
			memcpy(&tmp, datap, 4);
			cout << tmp << " ";
		}
		else if (AttrType[i] == "char")
		{
			char* tmp = new char[AttrLengths[i]];
			memcpy(tmp, datap, AttrLengths[i]);
			cout << tmp << " ";
		}
	}
	cout << endl;
}

bool isWhereOne(char* datap, vector<string> input, vector<int> Attr_Lengths, vector<string> Attr_Name,vector<string>Attr_Type)
{
	int i = 0;
	datap += 4;
	string name = input[2];

	for (;i < Attr_Name.size();i++)
	{
		if (name == Attr_Name[i]) break;
	}
	for (int j = 0;j < i;j++)
	{
		datap += Attr_Lengths[j];
	}
	if (Attr_Type[i] == "int")
	{
		int* value = new int;
		memcpy(value, datap, 4);
		if (judge(value, input) == true) return true;
		else return false;
	}
	else if (Attr_Type[i] == "float")
	{
		float* value = new float;
		memcpy(value, datap, 4);
		if (judge(value, input) == true) return true;
		else return false;
	}
	else if (Attr_Type[i] == "char")
	{
		char* value = new char[Attr_Lengths[i]];
		memcpy(value, datap, Attr_Lengths[i]);
		if (judge(value, input) == true) return true;
		else return false;
	}

}


bool isWhereTwo(char* datap, vector<string> input, vector<int> Attr_Lengths, vector<string> Attr_Name, vector<string>Attr_Type)
{
	vector<string> str1;
	str1.push_back(input[0]);
	str1.push_back(input[1]);
	str1.push_back(input[2]);
	str1.push_back(input[3]);
	vector<string>str2;
	str2.push_back(input[0]);
	str2.push_back(input[5]);
	str2.push_back(input[6]);
	str2.push_back(input[7]);
	string logic = input[4];
	if (logic == "and")
	{
		if (isWhereOne(datap, str1, Attr_Lengths, Attr_Name,Attr_Type) && isWhereOne(datap, str2, Attr_Lengths, Attr_Name,Attr_Type)) return true;
		else return false;
	}
	else if (logic == "or")
	{
		if (isWhereOne(datap, str1, Attr_Lengths, Attr_Name,Attr_Type) || isWhereOne(datap, str2, Attr_Lengths, Attr_Name,Attr_Type)) return true;
		else return false;
	}
}
bool judge(int* value, vector<string> input)
{
	string op = input[1];
	int comp = atoi(input[3].c_str());
	if (op == "=")
	{
		if (*value == comp) return true;
		else return false;

	}
	if (op == ">")
	{
		if (*value > comp) return true;
		else return false;
	}
	if (op == "<")
	{
		if (*value < comp) return true;
		else return false;
	}
	if (op == "<>")
	{
		if (*value != comp) return true;
		else return false;
	}
	if (op == ">=")
	{
		if (*value >= comp) return true;
		else return false;
	}
	if (op == "<=")
	{
		if (*value <= comp) return true;
		else return false;
	}
}
bool judge(float* value, vector<string> input)
{
	string op = input[1];
	float comp = atof(input[3].c_str());
	if (op == "=")
	{
		if (*value == comp) return true;
		else return false;

	}
	if (op == ">")
	{
		if (*value > comp) return true;
		else return false;
	}
	if (op == "<")
	{
		if (*value < comp) return true;
		else return false;
	}
	if (op == "<>")
	{
		if (*value != comp) return true;
		else return false;
	}
	if (op == ">=")
	{
		if (*value >= comp) return true;
		else return false;
	}
	if (op == "<=")
	{
		if (*value <= comp) return true;
		else return false;
	}
}
bool judge(char* value, vector<string> input)
{
	string op = input[1];
	const char* comp = input[3].c_str();
	string va_str = value;
	string co_str = comp;
	co_str = co_str.substr(1, co_str.length() - 2);
	if (op == "=")
	{
		if (va_str == co_str) return true;
		else return false;

	}
	if (op == ">")
	{
		if (va_str > co_str) return true;
		else return false;
	}
	if (op == "<")
	{
		if (va_str < co_str) return true;
		else return false;
	}
	if (op == "<>")
	{
		if (va_str != co_str) return true;
		else return false;
	}
	if (op == ">=")
	{
		if (va_str >= co_str) return true;
		else return false;
	}
	if (op == "<=")
	{
		if (va_str <= co_str) return true;
		else return false;
	}
}

void delete_record(string str)
{
	int tupleOffset = 4;
	vector<string> input = stringSplit(str);
	string table_name = input[0];
	int str_len = input.size();
	RecordBlock* de_record = BufferManager::returnFirstRecordBlock(table_name);
	int recordLen = returnRecordLen(table_name);
	int n = 1;
	char* datap = (char*)de_record->srcData;
	datap += tupleOffset;
	int isDelete;
	memcpy(&isDelete, datap, 4);
	if (str_len == 2)
	{
		while (datap - (char*)de_record->srcData <= 8192)
		{
			if (isDelete == 1) delete_tuple(datap, table_name, de_record);
			datap += recordLen + 4;
			memcpy(&isDelete, datap, 4);
		}
	}
	else if (str_len == 4)
	{
		vector<string> Attr_Name = returnAttriNames(table_name);
		vector<int> AttrLengths = returnAttrLengths(table_name);
		vector<string> Attr_Type = returnAttriType(table_name);
		while (datap - (char*)de_record->srcData <= 8192)
		{
			if (isDelete == 1)
			{
				if (isWhereOne(datap, input, AttrLengths, Attr_Name, Attr_Type) == true) delete_tuple(datap, table_name, de_record);
				datap += recordLen + 4;
				memcpy(&isDelete, datap, 4);
			}
			else datap += recordLen + 4;

		}

	}

	else if (str_len == 9)
	{
		vector<string> Attr_Name = returnAttriNames(table_name);
		vector<int> AttrLengths = returnAttrLengths(table_name);
		vector<string> Attr_Type = returnAttriType(table_name);
		while (datap - (char*)de_record->srcData <= 8192)
		{
			if (isDelete == 1)
			{
				if (isWhereTwo(datap, input, AttrLengths, Attr_Name, Attr_Type)) delete_tuple(datap, table_name, de_record);
				datap += recordLen + 4;
				memcpy(&isDelete, datap, 4);
			}
			else datap += recordLen + 4;
		}
	}
	RecordBlock* de_ne_record = BufferManager::returnNextRecordBlock(de_record->returnID());
	de_record->unpin();
	while (de_ne_record != nullptr)
	{
		int n_ne = 1;
		char* datap_ne = (char*)de_ne_record->srcData;
		datap_ne += tupleOffset;
		int isDelete_ne;
		memcpy(&isDelete_ne, datap_ne, 4);
		if (str_len == 2)
		{
			while (datap_ne - (char*)de_ne_record->srcData <= 8192)
			{
				if (isDelete_ne == 1) delete_tuple(datap_ne, table_name, de_record);
				datap_ne += recordLen + 4;
				memcpy(&isDelete_ne, datap_ne, 4);
			}
		}
		else if (str_len == 4)
		{
			vector<string> Attr_Name_ne = returnAttriNames(table_name);
			vector<int> AttrLengths_ne = returnAttrLengths(table_name);
			vector<string> Attr_Type_ne = returnAttriType(table_name);
			while (datap_ne - (char*)de_ne_record->srcData <= 8192)
			{
				if (isDelete_ne == 1)
				{
					if (isWhereOne(datap_ne, input, AttrLengths_ne, Attr_Name_ne, Attr_Type_ne) == true) delete_tuple(datap_ne, table_name, de_record);
					datap_ne += recordLen + 4;
					memcpy(&isDelete_ne, datap_ne, 4);
				}
				else datap_ne += recordLen + 4;
			}

		}

		else if (str_len == 9)
		{
			vector<string> Attr_Name_ne = returnAttriNames(table_name);
			vector<int> AttrLengths_ne = returnAttrLengths(table_name);
			vector<string> Attr_Type_ne = returnAttriType(table_name);
			while (datap - (char*)de_record->srcData <= 8192)
			{
				if (isDelete_ne == 1)
				{
					if (isWhereTwo(datap_ne, input, AttrLengths_ne, Attr_Name_ne, Attr_Type_ne)) delete_tuple(datap, table_name, de_record);
					datap_ne += recordLen + 4;
					memcpy(&isDelete_ne, datap_ne, 4);
				}
				else datap_ne += recordLen + 4;
			}
		}
		de_ne_record->unpin();
		de_ne_record = BufferManager::returnNextRecordBlock(de_ne_record->returnID());
	}

	

}

void delete_tuple(char* datap, string table_name, RecordBlock* de_record)
{
	int flag = 0;
	memcpy(datap, &flag, 4);
	de_record->returnFilePtr()->insertFreeSlotOffset(datap - de_record->srcData + de_record->returnOffset());
	recordNumDecrease(table_name);
	vector<int> AttrLengths = returnAttrLengths(table_name);
	int i = 0, j = 1;
	de_record->recordNum--;
	de_record->setDirty();
	de_record->unpin();
	BufferManager::updateUsedTime(de_record->returnID());
	return;
}