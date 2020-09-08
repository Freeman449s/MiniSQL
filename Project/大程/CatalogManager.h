#ifndef _CATALOGMANAGER_H_
#define _CATALOGMANAGER_H_

// (C) 2020, Gaoyuan Wu, Minzhe Tang
// CatalogManager.h
// All rights reserved.

#include <string>
#include <iostream>
#include <vector>
#include <fstream>
using namespace std;

#define MAX_INT 2147483647

class Attribute {
public:
	string attr_name;
	int attr_type;				//属性的数据类型，int为1，float为2，char为3
	int attr_key_type;			//属性的主键类型，primary key为1，unique为2，都不是则为0
	int attr_len;				//属性的长度，不是char则为4。若为char类型，需要将结束符也计算在内
	string attr_index;			//索引名
};

class Table {
public:
	string table_name;
	int attr_num;				//表中属性的总个数
	int record_num = 0;         //表中的记录数
	int record_len;				//元组长度，不包含删除标记位，包含字符串的结束符位
	vector<Attribute> attrs;	//表的所有属性列表
};

class TableFile {
public:
	int table_num;			//表的数量
	vector<Table> table;	//表对象的向量
};

//returnIndexType函数需要返回一个整型指明索引的类型。1->整型，2->浮点型，3->字符串型
int returnIndexType(string tableName, string attriName);

//returnKeyLen函数需要返回属性对应的数据类型的长度（字节）。
int returnKeyLen(string tableName, string attriName);

//isAttriIndexed函数需要告知属性上是否有索引
bool isAttriIndexed(string tableName, string attriName);

//doesTableExist函数需要告知表是否存在
bool doesTableExist(string tableName);

//doesAttriExist函数
bool doesAttriExist(string tableName, string attriName);

//returnRecordLen函数需要返回元组的长度
//不包含起始用于标记元组是否删除的数据位。若存在字符串型属性，结束符位需要一并包含。
int returnRecordLen(string tableName);

//returnIndexAttriList需要返回表上建有索引的属性名的列表，存储于向量中
//调用者BufferManager::dropTable保证表名存在
vector<string> returnIndexedAttriList(string tableName);

//顺序返回表上各属性的长度
vector<int> returnAttrLengths(string tableName);

//返回属性数据在元组中的偏移量
int returnAttriPosInTuple(string tableName, string attriName);

//返回属性名向量
vector<string> returnAttriNames(string tableName);

//返回属性数据类型
//为方便调用者实现，直接返回"int","float"和"char"
vector<string> returnAttriType(string tableName);

//举例：cin_string为: table index name ，那么seperate_word(cin_string,2)返回值为index, seperate_word(cin_string,3)返回值为name，即用于分离有空格的字符串
string seperate_word(string cin_string, int num);

//读数据字典
void read_dict_file();
//写数据字典
void write_dict_file();

//将长串数字字符串转化为int类型
bool string_to_int(string int_string, int & result);

/*输入值input为： 表名 属性名 类型 （unique） 属性名 类型 （unique）······ 主键
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
//注意：为保证数据写回磁盘，必须在函数结尾调用write_dict_file
bool create_table(string input, int attr_num);

/*输入值为：索引名 表名 属性名
例：create index sdafsf on box (dfa) ;
返回值为 sdafsf box dfa       */
bool create_index(string input);

/*输入值为：表名*/
bool drop_table(string input);

/*输入值为：索引名*/
bool drop_index(string input);

//告知属性是否是unique属性
bool return_if_unique(string tableName, string attriName);

//stringSplit函数按ch分割字符串
vector<string> stringSplit(const string& str);

//isReservedWord在解析指令字符串时使用，当str为"int","float","char","unique"时返回true
bool isReservedWord(string str);

//用于更新Catalog中记录的表的记录数，每调用一次函数增减1
bool recordNumIncrease(string tableName);
bool recordNumDecrease(string tableName);


void delete_first_space(string & string_input);
void delete_last_space(string & string_input);

bool select_record_true(string input);
bool delete_record_true(string input);
bool insert_record_true(string input);

#endif