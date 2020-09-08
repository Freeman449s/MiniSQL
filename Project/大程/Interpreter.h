#pragma once
#include"RecordManager.h"
#include <iostream>
#include <string>
#include <fstream>
using namespace std;

#define ERROR_WRONG_INPUT     "�������룡"
#define ERROR_USELESS_INPUT   "��Ч�����"
#define ERROR_FILE_NOT_EXIST "�޷����ļ���"
#define ERROR_NOTFULL_CMD "������ļ����"
#define ERROR_USELESS_CREATE "�����create���"
#define ERROR_USELESS_DELETE "�����delete���"
#define ERROR_USELESS_DROP "�����drop���"
#define ERROR_USELESS_INSERT "�����insert���"
#define ERROR_USELESS_SELECT "�����select���"
#define ERROR_TABLE_NOT_EXIST "�˱����ڣ�"
#define ERROR_INDEX_NOT_EXIST "�����������ڣ�"
#define ERROR_MAKING_CREATE_TABLE "�˱��޷�������"
#define ERROR_MAKING_CREATE_INDEX "�������޷�������"
#define ERROE_CREATE_INDEX "��Ч�Ĵ����������"
#define ERROE_CREATE_TABLE "��Ч�Ĵ��������"
#define ERROR_INSERT_WRONG "��Ч�Ĳ������"





#define CREATE_TABLE         1
#define CREATE_INDEX         2
#define CREATE_DATABASE      3
#define DELETE_TABLE         4
#define DROP_TABLE           5
#define DROP_INDEX           6
#define DROP_DATABASE        7


string error_info;					//��¼������Ϣ
string translated_record;           //�������ļ�¼
int translated_record_type;         //��¼������
string check_delete_type(string input);
void use_interpreter();
bool get_input(string & user_input);
bool check_type(string user_input);
bool string_to_int(string int_string, int & result);
bool check_unique(string & string_input);
bool check_create(string user_input, int &attr_num);
string check_drop(string user_input);
string check_insert(string user_input);
string check_create_table(string user_input, int &attr_num);
string check_create_index(string user_input);
//string check_create_database(string user_input);//database������ʵ��
string check_delete(string user_input);
string check_select(string user_input);
bool open_execfile(string & file_input);
void output_select_ans();



