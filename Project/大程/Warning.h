#ifndef _WARNING_H_
#define _WARNING_H_

#include <string>
#include <iostream>
using namespace std;

//函数返回值表明函数异常时显示的警告
void functionFailureWarning(string funName, string failureFunName);

//Catalog Manager表示表不存在时的警告
void tableNotExistWarning(string funName, string tableName);

//Catalog Manager表示属性不存在时的警告
void attriNotExistWarning(string funName, string tableName, string attriName);

//文件无法打开时的警告
void fileCannotOpenWarning(string funName, string fileName);

//程序可能出现内部错误时的警告
void internalErrorWarning(string funName, string description);

//偏移量非法时的警告
void offsetOutOfRangeWarning(string funName);

//关闭文件或关闭数据库时，有块仍未解锁的警告
void blockPinnedWarning(string funName, string fileName);

//传入块id非法的警告
void invalidIDWarning(string funName);

//试图删除索引时，若属性不存在或未加索引时显示的警告
void attriNotIndexedWarning(string funName, string tableName, string attriName);

// ========================= Index Manager 加入的警告 ====================================

//属性非key
void attriNotUniqueWarning(string funName);

//key值重复
void keyDuplicateWarning(string funName);

//二次索引溢出
void rootFullWarning(string funName);

//删除时key值不存在
void keyNotFoundWarning(string funName);

// ======================================================================================

#endif // !_WARNING_H_