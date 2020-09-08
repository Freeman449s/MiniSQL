#ifndef INDEXMANAGER_H
#define INDEXMANAGER_H

#include <string>
#include "BufferManager.h"
#include "CatalogManager.h"
#include "Warning.h"
using namespace std;
//目前方便起见统一全部B+树的节点度数与分裂度数
//因为改成二级索引了，这里稍作调整
#define MAX_keyNum 1000
#define MIN_keyNum 1
#define LEFT_keyNum 500
#define RIGHT_keyNum 501

//节点：IndexBlock（的结构？

//关键数据：
// int nextSibling = -1		 					
// 似乎不需要父节点，我改成next指针，0代表是非叶子节点，正值表示next的offset，-1表示最右的叶子节点
// 默认值也要改为-1，因为单根节点应视为单叶子节点，next为-1
// 发现在建立树的时候还是需要的，但是似乎没地方存了，暂时假设有两个变量来做吧
//	int parent = -1;					// 父节点指针，需要写入磁盘。默认值-1
// 									    // 在磁盘中存储的字典信息，顺序为parent->keyNum
// int keyNum = 0;                      // 块中存储的key的数量，需要写入磁盘。默认值0

// int* ptrs = nullptr;                 // 指针数组，实际是存储offset的数组
// 									    // 在磁盘中，指针先于key存储
// 在我的理解范围内，叶子节点的ptrs是没有内容的。
// int* keyi = nullptr;				    // 整型key数组。最多允许1022个整型key。
// float* keyf = nullptr;			    // 浮点key数组。最多允许1022个浮点型key。
// char** keystr = nullptr;			    // 字符串型key数组，最大key数量动态确定。

bool create_tree(string tableName, string attriName);
int search(string tableName, string attriName, void* value, int& offset);
int search_range(string tableName, string attriName, int SEARCH_TYPE, void* value, int& begin_offset, int& end_offset);
int insert_node(string tableName, string attriName, void* value);
int delete_node(string tableName, string attriName, void* value);

#endif