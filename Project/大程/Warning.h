#ifndef _WARNING_H_
#define _WARNING_H_

#include <string>
#include <iostream>
using namespace std;

//��������ֵ���������쳣ʱ��ʾ�ľ���
void functionFailureWarning(string funName, string failureFunName);

//Catalog Manager��ʾ������ʱ�ľ���
void tableNotExistWarning(string funName, string tableName);

//Catalog Manager��ʾ���Բ�����ʱ�ľ���
void attriNotExistWarning(string funName, string tableName, string attriName);

//�ļ��޷���ʱ�ľ���
void fileCannotOpenWarning(string funName, string fileName);

//������ܳ����ڲ�����ʱ�ľ���
void internalErrorWarning(string funName, string description);

//ƫ�����Ƿ�ʱ�ľ���
void offsetOutOfRangeWarning(string funName);

//�ر��ļ���ر����ݿ�ʱ���п���δ�����ľ���
void blockPinnedWarning(string funName, string fileName);

//�����id�Ƿ��ľ���
void invalidIDWarning(string funName);

//��ͼɾ������ʱ�������Բ����ڻ�δ������ʱ��ʾ�ľ���
void attriNotIndexedWarning(string funName, string tableName, string attriName);

// ========================= Index Manager ����ľ��� ====================================

//���Է�key
void attriNotUniqueWarning(string funName);

//keyֵ�ظ�
void keyDuplicateWarning(string funName);

//�����������
void rootFullWarning(string funName);

//ɾ��ʱkeyֵ������
void keyNotFoundWarning(string funName);

// ======================================================================================

#endif // !_WARNING_H_