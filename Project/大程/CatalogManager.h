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
	int attr_type;				//���Ե��������ͣ�intΪ1��floatΪ2��charΪ3
	int attr_key_type;			//���Ե��������ͣ�primary keyΪ1��uniqueΪ2����������Ϊ0
	int attr_len;				//���Եĳ��ȣ�����char��Ϊ4����Ϊchar���ͣ���Ҫ��������Ҳ��������
	string attr_index;			//������
};

class Table {
public:
	string table_name;
	int attr_num;				//�������Ե��ܸ���
	int record_num = 0;         //���еļ�¼��
	int record_len;				//Ԫ�鳤�ȣ�������ɾ�����λ�������ַ����Ľ�����λ
	vector<Attribute> attrs;	//������������б�
};

class TableFile {
public:
	int table_num;			//�������
	vector<Table> table;	//����������
};

//returnIndexType������Ҫ����һ������ָ�����������͡�1->���ͣ�2->�����ͣ�3->�ַ�����
int returnIndexType(string tableName, string attriName);

//returnKeyLen������Ҫ�������Զ�Ӧ���������͵ĳ��ȣ��ֽڣ���
int returnKeyLen(string tableName, string attriName);

//isAttriIndexed������Ҫ��֪�������Ƿ�������
bool isAttriIndexed(string tableName, string attriName);

//doesTableExist������Ҫ��֪���Ƿ����
bool doesTableExist(string tableName);

//doesAttriExist����
bool doesAttriExist(string tableName, string attriName);

//returnRecordLen������Ҫ����Ԫ��ĳ���
//��������ʼ���ڱ��Ԫ���Ƿ�ɾ��������λ���������ַ��������ԣ�������λ��Ҫһ��������
int returnRecordLen(string tableName);

//returnIndexAttriList��Ҫ���ر��Ͻ������������������б��洢��������
//������BufferManager::dropTable��֤��������
vector<string> returnIndexedAttriList(string tableName);

//˳�򷵻ر��ϸ����Եĳ���
vector<int> returnAttrLengths(string tableName);

//��������������Ԫ���е�ƫ����
int returnAttriPosInTuple(string tableName, string attriName);

//��������������
vector<string> returnAttriNames(string tableName);

//����������������
//Ϊ���������ʵ�֣�ֱ�ӷ���"int","float"��"char"
vector<string> returnAttriType(string tableName);

//������cin_stringΪ: table index name ����ôseperate_word(cin_string,2)����ֵΪindex, seperate_word(cin_string,3)����ֵΪname�������ڷ����пո���ַ���
string seperate_word(string cin_string, int num);

//�������ֵ�
void read_dict_file();
//д�����ֵ�
void write_dict_file();

//�����������ַ���ת��Ϊint����
bool string_to_int(string int_string, int & result);

/*����ֵinputΪ�� ���� ������ ���� ��unique�� ������ ���� ��unique�������������� ����
attr_numΪ��������
����create table t1
(
	id int,
	name char(20) unique,
	age int,
	salary float,
	primary key(id)
	);
inputΪ��t1 id int unique name char 20 age int salary float id
ע�⣬��Ϊchar(20)����ֵΪchar 20,�ڶ�ȡʱ��������char���͵�ʱ��ֻ���ٶ�һ�����ֽ������ɣ�
��������string_to_int���ڽ�string����ת��Ϊint����*/
//ע�⣺Ϊ��֤����д�ش��̣������ں�����β����write_dict_file
bool create_table(string input, int attr_num);

/*����ֵΪ�������� ���� ������
����create index sdafsf on box (dfa) ;
����ֵΪ sdafsf box dfa       */
bool create_index(string input);

/*����ֵΪ������*/
bool drop_table(string input);

/*����ֵΪ��������*/
bool drop_index(string input);

//��֪�����Ƿ���unique����
bool return_if_unique(string tableName, string attriName);

//stringSplit������ch�ָ��ַ���
vector<string> stringSplit(const string& str);

//isReservedWord�ڽ���ָ���ַ���ʱʹ�ã���strΪ"int","float","char","unique"ʱ����true
bool isReservedWord(string str);

//���ڸ���Catalog�м�¼�ı�ļ�¼����ÿ����һ�κ�������1
bool recordNumIncrease(string tableName);
bool recordNumDecrease(string tableName);


void delete_first_space(string & string_input);
void delete_last_space(string & string_input);

bool select_record_true(string input);
bool delete_record_true(string input);
bool insert_record_true(string input);

#endif