// (C) 2020, Gaoyuan Wu, Minzhe Tang
// CatalogManager.cpp
// All rights reserved.

#include "CatalogManager.h"
#include "BufferManager.h"
#include "Warning.h"

//数据字典的类型如下：
//table_num(表数量)  
//表1名字 表1属性数 表1实际记录长度  属性名 数据类型 数据长度 属性类型 索引名  属性名 数据类型 数据长度 属性类型 索引名 .....
//表2名字 表2属性数 表2实际记录长度  属性名 数据类型 数据长度 属性类型 索引名  属性名 数据类型 数据长度 属性类型 索引名 .....
//.......
TableFile table_file;
void read_dict_file() {
	fstream file("data.txt");
	int table_num;
	if (!file) {//如果不存在data.txt，那么创建一个，并写入table_num为0
		file << 0;
		file.close();
	}
	else {//将数据读入
		file >> table_num;//读入表的数量
		table_file.table_num = table_num;
		for (int i = 0; i < table_num; i++) {
			Table table;
			file >> table.table_name;//读入表名
			file >> table.attr_num;//读入属性数量
			file >> table.record_len;//读入元组长
			file >> table.record_num;//读入表的元组数
			for (int j = 0; j < table.attr_num; j++) {
				Attribute attr;
				file >> attr.attr_name;//读入属性名
				file >> attr.attr_type;//读入数据类型
				file >> attr.attr_len;//读入数据长度
				file >> attr.attr_key_type;//读入属性类型
				file >> attr.attr_index;//读入索引名
				table.attrs.push_back(attr);
			}
			table_file.table.push_back(table);
		}
		file.close();
	}
}

void write_dict_file() {
	ofstream file("data.txt");
	file << table_file.table_num << endl;
	for (int i = 0; i < table_file.table_num; i++) {
		Table table = table_file.table[i];
		file << table.table_name << " ";//输出表名
		file << table.attr_num << " ";//输出属性数量
		file << table.record_len << " ";//输出元组长
		file << table.record_num << " ";//输出表的元组数
		for (int j = 0; j < table.attr_num; j++) {
			Attribute attr = table.attrs[j];
			file << attr.attr_name << " ";//输出属性名
			file << attr.attr_type << " ";//输出数据类型
			file << attr.attr_len << " ";//输出数据长度
			file << attr.attr_key_type << " ";//输出属性类型
			file << attr.attr_index << " ";//输出索引名
		}
		file << endl;
	}
	file.close();
}

//此函数用于分离用户输入的字符串，num是指分离出其中的第num个word
string seperate_word(string cin_string, int num) {
	delete_first_space(cin_string);
	delete_last_space(cin_string);
	int begin = 0, length = 0;
	bool found = false;
	int tmp_num = 1;//记录已经读到了第几个word
	for (int i = 0; i < cin_string.length(); i++) {
		if (i > 0 && cin_string.at(i) == ' ' && cin_string.at(i - 1) != ' ') {
			tmp_num++;
			while (i < cin_string.length()) {
				if (cin_string.at(i + 1) == ' ') i++;
				else break;
			}
			i++;
		}
		if (tmp_num == num) {
			begin = i;
			found = true;
			break;
		}
	}
	if (found == false) return "";
	string cout_word;
	for (int i = begin; i < cin_string.length() - 1; i++) {
		if (cin_string.at(i) != ' ' && cin_string.at(i + 1) == ' ') break;
		else length++;
	}
	cout_word.assign(cin_string, begin, length + 1);
	return cout_word;
}

//将长串数字字符串转化为int类型
bool string_to_int(string int_string, int& result) {
	result = 0;
	for (int i = 0; i < int_string.length(); i++) {
		int temp = 0;
		if (int_string.at(i) == '0')    temp = 0;
		else if (int_string.at(i) == '1')        temp = 1;
		else if (int_string.at(i) == '2')        temp = 2;
		else if (int_string.at(i) == '3')        temp = 3;
		else if (int_string.at(i) == '4')        temp = 4;
		else if (int_string.at(i) == '5')        temp = 5;
		else if (int_string.at(i) == '6')        temp = 6;
		else if (int_string.at(i) == '7')        temp = 7;
		else if (int_string.at(i) == '8')        temp = 8;
		else if (int_string.at(i) == '9')        temp = 9;
		else        return false;
		result = 10 * result + temp;
	}
	return true;
}

vector<string> stringSplit(const string& str) {
	int blankspacePos;
	int offset = 0;
	vector<string> ret;
	int i = 1;
	while (1) {
		string temp = seperate_word(str, i);
		if (temp != "") {
			ret.push_back(seperate_word(str, i));
			i++;
		}
		else break;
	}
	return ret;
}

//returnIndexType函数需要返回一个整型指明索引的类型。1->整型，2->浮点型，3->字符串型
int returnIndexType(string tableName, string attriName) {
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			Table& table = table_file.table[i];
			for (int j = 0; j < table.attrs.size(); j++) {
				//调用者IndexBlock::parseData()保证表名和属性名存在
				if (table.attrs[j].attr_name == attriName) return table.attrs[j].attr_type;
			}
		}
	}
}

//returnKeyLen函数需要返回属性对应的数据类型的长度（字节）。
int returnKeyLen(string tableName, string attriName) {
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			Table& table = table_file.table[i];
			for (int j = 0; j < table.attrs.size(); j++) {
				if (table.attrs[j].attr_name == attriName) return table.attrs[j].attr_len;
			}
			return -1;
		}
	}
	return -1;
}

//doesTableExist函数需要告知表是否存在
bool doesTableExist(string tableName) {
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) return true;
	}
	return false;
}

//doesAttriExist函数
bool doesAttriExist(string tableName, string attriName) {
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			const Table& table = table_file.table[i];
			for (int j = 0; j < table.attrs.size(); j++) {
				if (table.attrs[j].attr_name == attriName) return true;
			}
			return false;
		}
	}
	return false;
}

//returnRecordLen函数需要返回元组的长度
//不包含起始用于标记元组是否删除的数据位。若存在字符串型属性，结束符位需要一并包含。
int returnRecordLen(string tableName) {
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) return table_file.table[i].record_len;
	}
}

//returnIndexedAttriList需要返回表上建有索引的属性名的列表，存储于向量中
//调用者BufferManager::dropTable保证表名存在
vector<string> returnIndexedAttriList(string tableName) {
	vector<string> ret;
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			const Table& table = table_file.table[i];
			for (int j = 0; j < table.attrs.size(); j++) {
				if (table.attrs[j].attr_index != ";") ret.push_back(table.attrs[j].attr_name);
			}
		}
	}
	return ret;
}

vector<int> returnAttrLengths(string tableName) {
	vector<int> ret;
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			const Table& table = table_file.table[i];
			for (int j = 0; j < table.attrs.size(); j++) {
				ret.push_back(table.attrs[j].attr_len);
			}
			break;
		}
	}
	return ret;
}

int returnAttriPosInTuple(string tableName, string attriName) {
	int offset = 0;
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			const Table& table = table_file.table[i];
			for (int j = 0; j < table.attrs.size(); j++) {
				if (!(table.attrs[j].attr_name == attriName)) {
					offset += table.attrs[j].attr_len;
				}
				else return offset;
			}
			//属性不存在
			return -1;
		}
	}
	//表不存在
	return -1;
}

vector<string> returnAttriNames(string tableName) {
	vector<string> ret;
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			const Table& table = table_file.table[i];
			for (int j = 0; j < table.attrs.size(); j++) ret.push_back(table.attrs[j].attr_name);
		}
	}
	return ret;
}

vector<string> returnAttriType(string tableName) {
	vector<string> ret;
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			const Table& table = table_file.table[i];
			for (int j = 0; j < table.attrs.size(); j++) {
				int type = table.attrs[j].attr_type;
				if (type == 1) ret.push_back("int");
				else if (type == 2) ret.push_back("float");
				else if (type == 3) ret.push_back("char");
			}
		}
	}
	return ret;
}

/*输入值input为： 表名 属性名 类型 （unique） 属性名 类型 （unique）······ (主键 ;)
attr_num为属性数量
例：create table t1
(
	id int,
	name char(20) unique,
	age int,
	salary float,
	primary key(id)
	);
input为：t1 id int unique name char 20 age int salary float id
注意，若为char(20)返回值为char 20,在读取时，当读到char类型的时候，只需再读一个数字进来即可，
另赠函数string_to_int用于将string类型转化为int类型*/
bool create_table(string input, int attr_num) {
	vector<string> strv = stringSplit(input);
	int index = 0;
	string tableName = strv[index++];
	string primaryKeyName = "";
	int primaryKeyLen = 0;
	int primaryKeyType = -1;
	//检查表是否已存在
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) return false;
	}
	Table newTable;
	//Table对象需要设置table_name，attr_num，record_len，attrs四个成员
	newTable.attr_num = attr_num;
	newTable.table_name = tableName;
	int recordLen = 0;
	while (index < strv.size()) {
		//属性名
		if (!isReservedWord(strv[index])) {
			string attrName = strv[index];
			//主键
			if (index == strv.size() - 2 && strv[index + 1] == ";") {
				primaryKeyName = attrName;
				for (int i = 0; i <= newTable.attrs.size() - 1; i++) {
					if (newTable.attrs[i].attr_name == attrName) {
						Attribute& attr = newTable.attrs[i];
						attr.attr_key_type = 1;
						//默认索引名：表名-属性名
						attr.attr_index = tableName + "-" + attrName;
						primaryKeyLen = attr.attr_len;
						primaryKeyType = attr.attr_type;
					}
				}
				break;
			}
			//新属性
			else {
				//需要设置attr_name，attr_type，attr_len，attr_key_type，attr_index五个成员
				Attribute newAttr;
				newAttr.attr_name = attrName;
				newAttr.attr_index = ";";
				index++;
				string type = strv[index++];
				if (type == "char") {
					newAttr.attr_type = 3;
					int length;
					string_to_int(strv[index++], length);
					if (length > 255 || length < 1) return false;
					newAttr.attr_len = length;
					recordLen += length;
				}
				else if (type == "int") {
					newAttr.attr_type = 1;
					newAttr.attr_len = 4;
					recordLen += 4;
				}
				else if (type == "float") {
					newAttr.attr_type = 2;
					newAttr.attr_len = 4;
					recordLen += 4;
				}
				if (index >= strv.size()) {
					newAttr.attr_key_type = 0;
					newTable.attrs.push_back(newAttr);
					break;
				}
				if (strv[index] != "unique") newAttr.attr_key_type = 0;
				else {
					newAttr.attr_key_type = 2;
					index++;
				}
				newTable.attrs.push_back(newAttr);
			}
		}
	}
	newTable.record_len = recordLen;

	table_file.table_num++;
	table_file.table.push_back(newTable);

	write_dict_file();

	if (!BufferManager::createRecordFile(tableName, recordLen)) return false;
	if (primaryKeyName.length() > 0) {
		if (!BufferManager::createIndexFile(tableName, primaryKeyName, primaryKeyType, primaryKeyLen)) return false;
	}

	return true;
}

/*输入值为：索引名 表名 属性名
例：create index sdafsf on box (dfa) ;
返回值为 sdafsf box dfa       */
//todo: 需要调用Index的函数在已有记录的属性上建立索引
bool create_index(string input) {
	vector<string> strv = stringSplit(input);
	string indexName = strv[0];
	string tableName = strv[1];
	string attrName = strv[2];
	//索引名为保留字
	if (indexName == ";") return false;
	//检查是否存在同名索引
	for (int i = 0; i < table_file.table.size(); i++) {
		for (int j = 0; j < table_file.table[i].attrs.size(); j++) {
			if (table_file.table[i].attrs[j].attr_index == indexName) return false;
		}
	}
	int indexedAttrType = -1;
	int indexedAttrLen = 0;
	vector<Table>::iterator tvit;
	for (tvit = table_file.table.begin(); tvit != table_file.table.end(); tvit++) {
		if ((*tvit).table_name == tableName) {
			Table& table = *tvit;
			vector<Attribute>::iterator avit;
			for (avit = table.attrs.begin(); avit != table.attrs.end(); avit++) {
				if ((*avit).attr_name == attrName) {
					//已经建立索引
					if ((*avit).attr_index != ";") return false;
					//属性不是unique属性
					if ((*avit).attr_key_type != 1 && (*avit).attr_key_type != 2) return false;
					(*avit).attr_index = indexName;
					indexedAttrType = (*avit).attr_type;
					indexedAttrLen = (*avit).attr_len;
					write_dict_file();
					if (!BufferManager::createIndexFile(tableName, attrName, indexedAttrType, indexedAttrLen)) return false;
					return true;
				}
			}
			//属性不存在
			return false;
		}
	}
	//表不存在
	return false;
}

/*输入值为：索引名*/
bool drop_index(string input) {
	int i, j;
	for (i = 0; i <= table_file.table.size() - 1; i++) {
		for (j = 0; j <= table_file.table[i].attrs.size() - 1; j++) {
			if (table_file.table[i].attrs[j].attr_index == input) {
				string tableName = table_file.table[i].table_name;
				string attriName = table_file.table[i].attrs[j].attr_name;
				if (!BufferManager::dropIndex(tableName, attriName)) {
					functionFailureWarning("drop_index", "BufferManager::dropIndex");
					return false;
				}
				table_file.table[i].attrs[j].attr_index = ";";
				write_dict_file();
				return true;
			}
		}
	}
	//索引不存在
	return false;
}

/*输入值为：表名*/
bool drop_table(string input) {
	vector<Table>::iterator tvit;
	for (tvit = table_file.table.begin(); tvit != table_file.table.end(); tvit++) {
		if ((*tvit).table_name == input) {
			if (!BufferManager::dropTable(input)) {
				functionFailureWarning("drop_table", "BufferManager::dropTable");
				return false;
			}
			table_file.table.erase(tvit);
			table_file.table_num--;
			write_dict_file();
			return true;
		}
	}
	//表不存在
	return false;
}

//告知属性是否是unique属性
bool return_if_unique(string tableName, string attriName) {
	for (int i = 0; i <= table_file.table.size() - 1; i++) {
		if (table_file.table[i].table_name == tableName) {
			const Table& table = table_file.table[i];
			for (int j = 0; j <= table.attrs.size() - 1; j++) {
				if (table.attrs[j].attr_name == attriName) {
					const Attribute& attr = table.attrs[j];
					if (attr.attr_key_type == 1 || attr.attr_key_type == 2) return true;
					else return false;
				}
			}
			//属性不存在
			return false;
		}
	}
	//表不存在
	return false;
}

//isReservedWord函数检查str是否是保留字，保留字包括int,float,char,unique
bool isReservedWord(string str) {
	if (str == "int") return true;
	if (str == "float") return true;
	if (str == "char") return true;
	if (str == "unique") return true;
	return false;
}

//isAttriIndexed函数需要告知属性上是否有索引
bool isAttriIndexed(string tableName, string attriName) {
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			const Table& table = table_file.table[i];
			for (int j = 0; j < table.attrs.size(); j++) {
				if (table.attrs[j].attr_name == attriName) {
					if (table.attrs[j].attr_index != ";") return true;
					else return false;
				}
			}
			//属性不存在
			return false;
		}
	}
	//表不存在
	return false;
}

//除去数据串结尾的所有空格（如果在输入中没有按规范操作输入，可能会产生空格,此函数用于处理这些空格）
void delete_last_space(string& string_input) {
	int i = string_input.length() - 1;
	while (string_input[i] == ' ') {
		string_input = string_input.substr(0, string_input.length() - 1);
	}
}
void delete_first_space(string& string_input) {
	int i = 0;
	while (string_input[i] == ' ') {
		string_input = string_input.substr(1, string_input.length() - 1);
	}
}

bool recordNumIncrease(string tableName) {
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			if (table_file.table[i].record_num < MAX_INT) {
				table_file.table[i].record_num++;
				return true;
			}
			//越界
			else return false;
		}
	}
	//表不存在
	return false;
}
bool recordNumDecrease(string tableName) {
	for (int i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == tableName) {
			if (table_file.table[i].record_num > 0) {
				table_file.table[i].record_num--;
				return true;
			}
			//越界
			else return false;
		}
	}
	//表不存在
	return false;
}

////output = "table asd adf sd sdaf sdfa ; "
//bool insert_record(string input) {
//	string table_name = seperate_word(input, 1);
//	if (!doesTableExist(table_name)) return false;
//	int iter = 2;
//	int i;
//	for (i = 0; i < table_file.table.size(); i++) {
//		if (table_file.table[i].table_name == table_name) {
//			break;
//		}
//	}
//	const Table& table = table_file.table[i];
//	int attr_num = 0;
//	while (1) {
//		string temp = seperate_word(input, iter);
//		iter++;
//		if (temp == ";") break;
//		else attr_num++;
//	}
//	if (attr_num != table.attr_num) return false;
//	iter = 2;
//	while (1) {
//		string temp = seperate_word(input, iter);
//		if (temp == ";") break;
//		else iter++;
//		if (table.attrs[iter - 3].attr_type == 1) {
//			for (int j = 0; j < temp.length(); j++) {
//				if (temp[j] < '0' || temp[j]>'9') return false;
//			}
//		}
//		else if (table.attrs[iter - 3].attr_type == 2) {
//			bool dot = false;
//			for (int j = 0; j < temp.length(); j++) {
//				if (temp[j] == '.') {
//					if (dot == false) dot = true;
//					else return false;
//				}
//				else if (temp[j] < '0' || temp[j]>'9') return false;
//			}
//		}
//		else if (table.attrs[iter - 3].attr_type == 3) {}
//		else return false;
//	}
//	return true;
//}
//
////table >= a b and <> c d  ;
//bool select_record(string input) {
//	string table_name = seperate_word(input, 1);
//
//	return true;
//}

//table >= a b and <> c d  ;
//table ;
bool select_record_true(string input) {
	string table_name = seperate_word(input, 1);
	if (!doesTableExist(table_name))
		return false;
	string temp = seperate_word(input, 2);
	if (temp == ";")return true;
	if (temp != "<" && temp != ">" && temp != "=" && temp != "<=" && temp != ">=" && temp != "<>") {
		return false;
	}
	string attr_name = seperate_word(input, 3);
	int i;
	for (i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == table_name) {
			break;
		}
	}
	const Table& table = table_file.table[i];
	for (i = 0; i < table.attr_num; i++) {
		if (table.attrs[i].attr_name == attr_name) {
			string temp = seperate_word(input, 4);
			if (table.attrs[i].attr_type == 1) {
				for (int j = 0; j < temp.length(); j++) {
					if (temp[j] < '0' || temp[j] > '9')
						return false;
				}
			}
			else if (table.attrs[i].attr_type == 2) {
				bool dot = false;
				for (int j = 0; j < temp.length(); j++) {
					if (temp[j] == '.') {
						if (dot == false)
							dot = true;
						else
							return false;
					}
					else if (temp[j] < '0' || temp[j] > '9')
						return false;
				}
			}
			else if (table.attrs[i].attr_type == 3) {
			}
			else
				return false;
			break;
		}
	}
	if (i == table.attr_num)
		return false;
	if (seperate_word(input, 5) == "")
		return true;
	else {
		string temp = seperate_word(input, 6);
		if (temp != "<" && temp != ">" && temp != "=" && temp != "<=" && temp != ">=" && temp != "<>") {
			return false;
		}
		string attr_name = seperate_word(input, 7);
		for (i = 0; i < table.attr_num; i++) {
			if (table.attrs[i].attr_name == attr_name) {
				string temp = seperate_word(input, 8);
				if (table.attrs[i].attr_type == 1) {
					for (int j = 0; j < temp.length(); j++) {
						if (temp[j] < '0' || temp[j] > '9')
							return false;
					}
				}
				else if (table.attrs[i].attr_type == 2) {
					bool dot = false;
					for (int j = 0; j < temp.length(); j++) {
						if (temp[j] == '.') {
							if (dot == false)
								dot = true;
							else
								return false;
						}
						else if (temp[j] < '0' || temp[j] > '9')
							return false;
					}
				}
				else if (table.attrs[i].attr_type == 3) {
				}
				else
					return false;
				break;
			}
		}
		if (i == table.attr_num)
			return false;
	}
	return true;
}

bool delete_record_true(string input) {
	if (seperate_word(input, 2) == ";") {
		string table_name = seperate_word(input, 1);
		if (!doesTableExist(table_name))
			return false;
		return true;
	}
	else {
		string table_name = seperate_word(input, 1);
		if (!doesTableExist(table_name))
			return false;
		string temp = seperate_word(input, 2);
		if (temp != "<" && temp != ">" && temp != "=" && temp != "<=" && temp != ">=" && temp != "<>") {
			return false;
		}
		string attr_name = seperate_word(input, 3);
		int i;
		for (i = 0; i < table_file.table.size(); i++) {
			if (table_file.table[i].table_name == table_name) {
				break;
			}
		}
		const Table& table = table_file.table[i];
		for (i = 0; i < table.attr_num; i++) {
			if (table.attrs[i].attr_name == attr_name) {
				string temp = seperate_word(input, 4);
				if (table.attrs[i].attr_type == 1) {
					for (int j = 0; j < temp.length(); j++) {
						if (temp[j] < '0' || temp[j] > '9')
							return false;
					}
				}
				else if (table.attrs[i].attr_type == 2) {
					bool dot = false;
					for (int j = 0; j < temp.length(); j++) {
						if (temp[j] == '.') {
							if (dot == false)
								dot = true;
							else
								return false;
						}
						else if (temp[j] < '0' || temp[j] > '9')
							return false;
					}
				}
				else if (table.attrs[i].attr_type == 3) {
				}
				else
					return false;
				break;
			}
		}
		if (i == table.attr_num)
			return false;
		if (seperate_word(input, 5) == "")
			return true;
		else {
			string temp = seperate_word(input, 6);
			if (temp != "<" && temp != ">" && temp != "=" && temp != "<=" && temp != ">=" && temp != "<>") {
				return false;
			}
			string attr_name = seperate_word(input, 7);
			for (i = 0; i < table.attr_num; i++) {
				if (table.attrs[i].attr_name == attr_name) {
					string temp = seperate_word(input, 8);
					if (table.attrs[i].attr_type == 1) {
						for (int j = 0; j < temp.length(); j++) {
							if (temp[j] < '0' || temp[j] > '9')
								return false;
						}
					}
					else if (table.attrs[i].attr_type == 2) {
						bool dot = false;
						for (int j = 0; j < temp.length(); j++) {
							if (temp[j] == '.') {
								if (dot == false)
									dot = true;
								else
									return false;
							}
							else if (temp[j] < '0' || temp[j] > '9')
								return false;
						}
					}
					else if (table.attrs[i].attr_type == 3) {
					}
					else
						return false;
					break;
				}
			}
			if (i == table.attr_num)
				return false;
		}
		return true;
	}
}

//output = "table asd adf sd sdaf sdfa ; "
bool insert_record_true(string input) {
	string table_name = seperate_word(input, 1);
	if (!doesTableExist(table_name))
		return false;
	int iter = 2;
	int i;
	for (i = 0; i < table_file.table.size(); i++) {
		if (table_file.table[i].table_name == table_name) {
			break;
		}
	}
	const Table& table = table_file.table[i];
	int attr_num = 0;
	while (1) {
		string temp = seperate_word(input, iter);
		iter++;
		if (temp == ";")
			break;
		else
			attr_num++;
	}
	if (attr_num != table.attr_num)
		return false;
	iter = 2;
	while (1) {
		string temp = seperate_word(input, iter);
		if (temp == ";")
			break;
		else
			iter++;
		if (table.attrs[iter - 3].attr_type == 1) {
			for (int j = 0; j < temp.length(); j++) {
				if (temp[j] < '0' || temp[j] > '9')
					return false;
			}
		}
		else if (table.attrs[iter - 3].attr_type == 2) {
			bool dot = false;
			for (int j = 0; j < temp.length(); j++) {
				if (temp[j] == '.') {
					if (dot == false)
						dot = true;
					else
						return false;
				}
				else if (temp[j] < '0' || temp[j] > '9')
					return false;
			}
		}
		else if (table.attrs[iter - 3].attr_type == 3) {
		}
		else
			return false;
	}
	return true;
}