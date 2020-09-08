#include "IndexManager.h"

// 在属性上建立树，调用
// 返回是否成功
// 如何保留根节点？
bool create_tree(string tableName, string attriName){
    // 属性非key
    if(!return_if_unique(tableName, attriName)){
        attriNotUniqueWarning("IndexManager::create_tree");
		return false;
    }
    // 已经存在索引？ ...
    /*
    if(isAttriIndexed(tableName, attriName)){
        //已经存在索引，不能建树
        //这个函数我没找到，问题不大之后考虑
    }
    */
    RecordBlock* recordblock = BufferManager::returnFirstRecordBlock(tableName);
    int ID = recordblock->returnID();
    IndexBlock* indexblock = BufferManager::returnFreeIndexBlock(tableName, attriName);
    indexblock->setRoot();//设为根
    int attri_pos = returnAttriPosInTuple(tableName, attriName);//属性位置
    int attri_keyLen = returnKeyLen(tableName, attriName);//属性长度
    for(;;){
        //根据Record建立Index节点
        //遍历recordblock，逐个insert？好像不对，那我的原始数据哪里来？
        //暂且假设我能遍历该属性的所有值，用test表示，然后逐个insert
        //遍历方式可能错误
        //整理一下RecordBlock的内部结构，srcData中可以获得某属性的所有记录
        //一个srcData的内容是：
        //四字节的字典信息，之后跟记录，每条记录开头是四字节的删除记录信息，1表示存在
        int TYPE = returnIndexType(tableName, attriName);
        if(TYPE == 1){
            //四字节字典信息
            int valuei;//要得到的数据
            int tupleOffset = 4;//四字节的字典信息
            char* p = (char*)recordblock->srcData;
            p += tupleOffset;
            int recordLen = returnRecordLen(tableName);
            int isDeleted;
            for(int i = 1;i <= recordblock->recordNum;i++){
                //似乎应是1->recordNum
                memcpy(&isDeleted, p, 4);//获取删除记录信息
                tupleOffset += 4;
                p += tupleOffset;
                if(isDeleted == 0){
                    tupleOffset += recordLen;//下一条记录
                    p += tupleOffset;
                    continue;
                }else{
                    //这里获得valuei的话，要知道该属性的位置
                    int tmp_tupleOffset = 0;//本条记录中该属性值的offset
                    vector<int> attriLen;
                    attriLen = returnAttrLengths(tableName);
                    for(int i = 0;i < attri_pos;i++){
                        tmp_tupleOffset += attriLen[i];//vector的返回是0开始的
                    }
                    memcpy(&valuei, p + tmp_tupleOffset, 4);
                    insert_node(tableName, attriName, &valuei);
                    //下一条记录
                    tupleOffset += recordLen;
                    p += tupleOffset;
                }
            }
            delete[] p;
        }
        if(TYPE == 2){
            //同type 1
            float valuef;
            int tupleOffset = 4;
            char* p = (char*)recordblock->srcData;
            p += tupleOffset;
            int recordLen = returnRecordLen(tableName);
            int isDeleted;
            for(int i = 1;i <= recordblock->recordNum;i++){
                memcpy(&isDeleted, p, 4);
                tupleOffset += 4;
                p += tupleOffset;
                if(isDeleted == 0){
                    tupleOffset += recordLen;
                    p += tupleOffset;
                    continue;
                }else{
                    int tmp_tupleOffset = 0;
                    vector<int> attriLen;
                    attriLen = returnAttrLengths(tableName);
                    for(int i = 0;i < attri_pos;i++){
                        tmp_tupleOffset += attriLen[i];
                    }
                    memcpy(&valuef, p + tmp_tupleOffset, 4);
                    insert_node(tableName, attriName, &valuef);
                    tupleOffset += recordLen;
                    p += tupleOffset;
                }
            }
            delete[] p;
        }
        if(TYPE == 3){
            char* valuestr = new char[attri_keyLen];
            int tupleOffset = 4;
            char* p = (char*)recordblock->srcData;
            p += tupleOffset;
            int recordLen = returnRecordLen(tableName);
            int isDeleted;
            for(int i = 1;i <= recordblock->recordNum;i++){
                memcpy(&isDeleted, p, 4);
                tupleOffset += 4;
                p += tupleOffset;
                if(isDeleted == 0){
                    tupleOffset += recordLen;
                    p += tupleOffset;
                    continue;
                }else{
                    int tmp_tupleOffset = 0;
                    vector<int> attriLen;
                    attriLen = returnAttrLengths(tableName);
                    for(int i = 0;i < attri_pos;i++){
                        tmp_tupleOffset += attriLen[i];
                    }
                    memcpy(&valuestr, p + tmp_tupleOffset, attri_keyLen);
                    insert_node(tableName, attriName, &valuestr);
                    tupleOffset += recordLen;
                    p += tupleOffset;
                }
            }
            delete[] p;
            delete[] valuestr;
        }
        //下一个Record
        recordblock = BufferManager::returnNextRecordBlock(ID);
        if(recordblock == nullptr){
            break;
        }
        ID = recordblock->returnID();
    }
}

//查找
//输入：表名、属性名、void*格式的key值
//返回：对应叶子节点indexblock的i，-1表示没找到。在int& offset中返回块的offset
//如果是单根节点会输出根节点offset
int search(string tableName, string attriName, void* value, int& offset){
    IndexBlock* node = BufferManager::returnRoot(tableName, attriName);
    int TYPE = returnIndexType(tableName, attriName);
    if(TYPE == 1){
        char* p = (char*)value;
        int valuei;
        memcpy(&valuei, value, 4);
        //在非叶节点中寻找，到叶子节点才能知道找没找到，所以此处必有叶节点输出
        //没有做剪枝
        while(node->nextSibling == 0){
            int i;
            for(i = 0;i < node->keyNum;i++){
                if(node->keyi[i] > valuei){
                    break;
                }
            }
            node = BufferManager::returnIndexBlock(tableName, attriName, node->ptrs[i]);
        }
        //进入叶子节点
        int find = 0;
        int i;
        for(i = 0;i < node->keyNum;i++){
            if(node->keyi[i] == valuei){
                find = 1;
                break;
            }
        }
        offset = node->returnOffset();//方便起见均输出offset
        if(find){
            return i;
        }else{
            return -1;
        }
    }
    if(TYPE == 2){
        //同type 1
        char* p = (char*)value;
        float valuef;
        memcpy(&valuef, value, 4);
        while(node->nextSibling == 0){
            int i;
            for(i = 0;i < node->keyNum;i++){
                if(node->keyi[i] > valuef){
                    break;
                }
            }
            node = BufferManager::returnIndexBlock(tableName, attriName, node->ptrs[i]);
        }
        int find = 0;
        int i;
        for(i = 0;i < node->keyNum;i++){
            if(node->keyi[i] == valuef){
                find = 1;
                break;
            }
        }
        offset = node->returnOffset();
        if(find){
            return i;
        }else{
            return -1;
        }
    }
    if(TYPE == 3){
        //字符串的处理不知道是否正确
        char* p = (char*)value;
        char* valuestr = new char[node->KEY_LEN];
        memcpy(&valuestr, value, node->KEY_LEN);
        while(node->nextSibling == 0){
            int i;
            for(i = 0;i < node->keyNum;i++){
                if(strcmp(node->keystr[i], valuestr) < 0){
                    break;
                }
            }
            node = BufferManager::returnIndexBlock(tableName, attriName, node->ptrs[i]);
        }
        int find = 0;
        int i;
        for(i = 0;i < node->keyNum;i++){
            if(strcmp(node->keystr[i], valuestr) == 0){
                find = 1;
                break;
            }
        }
        offset = node->returnOffset();
        if(find){
            return i;
        }else{
            return -1;
        }
        delete[] valuestr;
    }
}

//范围查找
//输入：表名、属性名、SEARCHTYPE、void*格式的key值、前后范围i的引用用于输出
//SEARCHTYPE:
//-2 <
//-1 <=
//1 >=
//2 >
//返回: <查找时返回最大块中的i，反之最小块中的i
//-1 没有符合的范围，此时引用中应是-1
//<查找时begin是第一块offset，反之end是最末块offset
//比如如果是找到了小于某值，begin会是第一块的offset，end会是范围内的最后一个块的offset，返回值会是这个最后一块中的最后一个符合要求的i
int search_range(string tableName, string attriName, int SEARCH_TYPE, void* value, int& begin_offset, int &end_offset){
    IndexBlock* node = BufferManager::returnRoot(tableName, attriName);
    int TYPE = returnIndexType(tableName, attriName);
    if(TYPE == 1){
        char* p = (char*)value;
        int valuei;
        memcpy(&valuei, value, 4);
        int last_i;//前一块的最后一个i
        //直接进最左块向右找
        //似乎有点脱裤子放屁，不过好像不这么做就会有查找值在块的边缘的情况，也很烦
        //突然发现这样也有块边缘的情况，那没事了
        while(node->nextSibling == 0){
            node = BufferManager::returnIndexBlock(tableName, attriName, node->ptrs[0]);
        }
        if(SEARCH_TYPE < 0){
            //小于要看看第一个值，不符合返回-1，符合才去遍历
            if(node->keyi[0] > valuei || SEARCH_TYPE == -2 && node->keyi[0] == valuei){
                begin_offset = end_offset = -1;
                return -1;
            }
            begin_offset = node->returnOffset();
            //向右遍历，找结束块
            for(;;){
                int i;
                for(i = 0;i < node->keyNum;i++){
                    if(i == 0 && (node->keyi[i] > valuei || SEARCH_TYPE == -2 && node->keyi[i] == valuei)){
                        //第一块的第一个是进不来这里的，因为前面已经输出了
                        //后面的块遇到这种情况要返回上一个块=
                        return last_i;
                    }
                    //一般情况
                    if(node->keyi[i] > valuei || SEARCH_TYPE == -2 && node->keyi[i] == valuei){
                        return i - 1;
                    }
                }
                last_i = i - 1;//更新last_i方便下一块或退出后直接返回
                if(node->nextSibling != -1){
                    //去下一块
                    node = BufferManager::returnIndexBlock(tableName, attriName, node->nextSibling);
                }else{
                    break;
                }
            }
            //没返回说明所有值都符合要求
            end_offset = node->returnOffset();
            return last_i;
        }else{
            //我tm直接遍历
            //向右遍历，找开始块
            int first_i = -1;//要存下第一个i，找到最后的offset后再返回
            for(;;){
                int i;
                if(first_i == -1){
                    //这里找到以后每次遍历就不用判断了
                    for(i = 0;i < node->keyNum;i++){
                        //一般情况
                        if(node->keyi[i] > valuei || SEARCH_TYPE == 1 && node->keyi[i] == valuei){
                            begin_offset = node->returnOffset();
                            first_i = i;
                            break;
                        }
                    }
                }
                //直接去找最后一块
                if(node->nextSibling != -1){
                    //去下一块
                    node = BufferManager::returnIndexBlock(tableName, attriName, node->nextSibling);
                }else{
                    break;
                }
            }
            if(first_i == -1){
                //没找到，所有都不符合要求
                begin_offset = end_offset = -1;
                return -1;
            }else{
                //找到了，返回最后的offset，begin_offset不用动
                end_offset = node->returnOffset();
                return first_i;
            }
        }
    }
    if(TYPE == 2){
        //同type 1
        char* p = (char*)value;
        float valuef;
        memcpy(&valuef, value, 4);
        int last_i;
        while(node->nextSibling == 0){
            node = BufferManager::returnIndexBlock(tableName, attriName, node->ptrs[0]);
        }
        if(SEARCH_TYPE < 0){
            if(node->keyf[0] > valuef || SEARCH_TYPE == -2 && node->keyf[0] == valuef){
                begin_offset = end_offset = -1;
                return -1;
            }
            begin_offset = node->returnOffset();
            for(;;){
                int i;
                for(i = 0;i < node->keyNum;i++){
                    if(i == 0 && (node->keyf[i] > valuef || SEARCH_TYPE == -2 && node->keyf[i] == valuef)){
                        return last_i;
                    }
                    //一般情况
                    if(node->keyf[i] > valuef || SEARCH_TYPE == -2 && node->keyf[i] == valuef){
                        return i - 1;
                    }
                }
                last_i = i - 1;
                if(node->nextSibling != -1){
                    node = BufferManager::returnIndexBlock(tableName, attriName, node->nextSibling);
                }else{
                    break;
                }
            }
            return last_i;
        }else{
            int first_i = -1;
            for(;;){
                int i;
                if(first_i == -1){
                    for(i = 0;i < node->keyNum;i++){
                        if(node->keyf[i] > valuef || SEARCH_TYPE == 1 && node->keyf[i] == valuef){
                            begin_offset = node->returnOffset();
                            first_i = i;
                            break;
                        }
                    }
                }
                if(node->nextSibling != -1){
                    node = BufferManager::returnIndexBlock(tableName, attriName, node->nextSibling);
                }else{
                    break;
                }
            }
            if(first_i == -1){
                begin_offset = end_offset = -1;
                return -1;
            }else{
                end_offset = node->returnOffset();
                return first_i;
            }
        }
    }
    if(TYPE == 3){
        //同type 1
        char* p = (char*)value;
        char* valuestr = new char[node->KEY_LEN];
        memcpy(&valuestr, value, node->KEY_LEN);
        int last_i;
        while(node->nextSibling == 0){
            node = BufferManager::returnIndexBlock(tableName, attriName, node->ptrs[0]);
        }
        if(SEARCH_TYPE < 0){
            if(strcmp(node->keystr[0], valuestr) > 0 || SEARCH_TYPE == -2 && strcmp(node->keystr[0], valuestr) == 0){
                begin_offset = end_offset = -1;
                return -1;
            }
            begin_offset = node->returnOffset();
            for(;;){
                int i;
                for(i = 0;i < node->keyNum;i++){
                    if(i == 0 && (strcmp(node->keystr[i], valuestr) > 0 || SEARCH_TYPE == -2 && strcmp(node->keystr[i], valuestr) == 0)){
                        return last_i;
                    }
                    //一般情况
                    if(strcmp(node->keystr[i], valuestr) > 0 || SEARCH_TYPE == -2 && strcmp(node->keystr[i], valuestr) == 0){
                        return i - 1;
                    }
                }
                last_i = i - 1;
                if(node->nextSibling != -1){
                    node = BufferManager::returnIndexBlock(tableName, attriName, node->nextSibling);
                }else{
                    break;
                }
            }
            return last_i;
        }else{
            int first_i = -1;
            for(;;){
                int i;
                if(first_i == -1){
                    for(i = 0;i < node->keyNum;i++){
                        if(strcmp(node->keystr[i], valuestr) > 0 || SEARCH_TYPE == 1 && strcmp(node->keystr[i], valuestr) == 0){
                            begin_offset = node->returnOffset();
                            first_i = i;
                            break;
                        }
                    }
                }
                if(node->nextSibling != -1){
                    node = BufferManager::returnIndexBlock(tableName, attriName, node->nextSibling);
                }else{
                    break;
                }
            }
            if(first_i == -1){
                begin_offset = end_offset = -1;
                return -1;
            }else{
                end_offset = node->returnOffset();
                return first_i;
            }
        }
        delete[] valuestr;
    }
}

//插入一个值
//输入：表名、属性名、key值
//输出：-1表示不成功
int insert_node(string tableName, string attriName, void* value){
    //获取根节点
    //B+树插入过于阴间，考虑时间和能力改为了二级索引，实际也没差，除非你有1000000条以上记录
    IndexBlock* root = BufferManager::returnRoot(tableName, attriName);
    int valuei;
    float valuef;
    char* valuestr = new char[root->KEY_LEN];
    int TYPE = returnIndexType(tableName, attriName);
    //若为单根节点的树，插入值即可
    if(root->keyNum < MAX_keyNum && root->nextSibling == -1){
        if(TYPE == 1){
            char* p = (char*)value;
            memcpy(&valuei, value, 4);
            root->keyi[root->keyNum] = valuei;
            root->keyNum++;
        }
        if(TYPE == 2){
            char* p = (char*)value;
            memcpy(&valuef, value, 4);
            root->keyf[root->keyNum] = valuef;
            root->keyNum++;
        }
        if(TYPE == 3){
            char* p = (char*)value;
            memcpy(&valuestr, value, root->KEY_LEN);
            strcpy(root->keystr[root->keyNum], valuestr);
            root->keyNum++;
        }
    }
    //非单根节点的树，root必是索引节点
    //查找应插入的block
    int offset;
    int find_i = search(tableName, attriName, value, offset);
    //find_i存在说明key已存在，返回错误
    if(find_i > 0){
        keyDuplicateWarning("IndexManager::insert_node");
        return -1;
    }
    IndexBlock* node = BufferManager::returnIndexBlock(tableName, attriName, offset);
    if(node->keyNum < MAX_keyNum){
        //小于，直接插入，可能需要更新父节点的索引
        int if_need = 0;//记录一下是否需要更新父节点（小于的情况下
        if(TYPE == 1){
            int replace_value;//用来记录父节点里需要被换掉的值
            int tmp;
            int tmp_value = valuei;//用来更新数组的工具值
            for(int i = 0;i < node->keyNum;i++){
                if(node->keyi[i] > valuei){
                    tmp = node->keyi[i];
                    node->keyi[i] = tmp_value;
                    tmp_value = tmp;
                    if(i == 0){
                        if_need = 1;
                        replace_value = tmp_value;
                    }
                }
            }
            node->keyi[node->keyNum] = tmp_value;
            node->keyNum++;
            if(if_need){
                //更新父节点，用valuei替换replace_value
                IndexBlock* parent;
                //for(;;){ 改为二级索引，不需要循环
                parent = BufferManager::returnIndexBlock(tableName, attriName, parent->parent);
                for(int i = 0;i < parent->keyNum;i++){
                    if(parent->keyi[i] == replace_value){
                        parent->keyi[i] = valuei;
                        return 1;
                    }
                }
                //}
            }
        }
        if(TYPE == 2){
            //同type 1
            float replace_value;
            float tmp;
            float tmp_value = valuef;
            for(int i = 0;i < node->keyNum;i++){
                if(node->keyf[i] > valuef){
                    tmp = node->keyf[i];
                    node->keyf[i] = tmp_value;
                    tmp_value = tmp;
                    if(i == 0){
                        if_need = 1;
                        replace_value = tmp_value;
                    }
                }
            }
            node->keyf[node->keyNum] = tmp_value;
            node->keyNum++;
            if(if_need){
                IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, parent->parent);
                for(int i = 0;i < parent->keyNum;i++){
                    if(parent->keyf[i] == replace_value){
                        parent->keyf[i] = valuef;
                        return 1;
                    }
                }
            }
        }
        if(TYPE == 3){
            //同type 1
            char* replace_value = new char[node->KEY_LEN];
            char* tmp = new char[node->KEY_LEN];
            char* tmp_value = new char[node->KEY_LEN];
            strcpy(tmp_value, valuestr);
            for(int i = 0;i < node->keyNum;i++){
                if(strcmp(node->keystr[i], valuestr) > 0){
                    strcpy(tmp, node->keystr[i]);
                    strcpy(node->keystr[i], tmp_value);
                    strcpy(tmp_value, tmp);
                    if(i == 0){
                        if_need = 1;
                        strcpy(replace_value, tmp_value);
                    }
                }
            }
            strcpy(node->keystr[node->keyNum], tmp_value);
            node->keyNum++;
            if(if_need){
                IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, parent->parent);
                for(int i = 0;i < parent->keyNum;i++){
                    if(strcmp(parent->keystr[i], replace_value) == 0){
                        strcpy(parent->keystr[i], valuestr);
                        return 1;
                    }
                }
            }
            delete[] replace_value;
            delete[] tmp;
            delete[] tmp_value;
        }
    }else{//node->keyNum == MAX_keyNum
        if(TYPE == 1){
            IndexBlock* right_node = BufferManager::returnFreeIndexBlock(tableName, attriName);
            //赋值给右边节点
            int j = 0;//右边节点的i
            int index_value = node->keyi[LEFT_keyNum];//保留待会要用来更新父节点的索引值
            int p_index_value = right_node->returnOffset();//父节点的对应offset应指向当前node
            for(int i = 0;i < MAX_keyNum;i++){
                if(i >= LEFT_keyNum){
                    right_node->keyi[j] = node->keyi[i];
                    node->keyi[i] = 0;
                    j++;
                    //更新keyNum
                    node->keyNum--;
                    right_node->keyNum++;
                }
            }
            if(node->parent == -1){
                //若原先是一级索引，添加根节点成为二级索引
                IndexBlock* indexblock = BufferManager::returnFreeIndexBlock(tableName, attriName);
                indexblock->setRoot();//设为根
                indexblock->parent = -1;
                indexblock->nextSibling = 0;
                indexblock->keyNum = 1;
                indexblock->keyi[0] = index_value;
                indexblock->ptrs[0] = p_index_value;
                //更新两个子节点
                node->parent = indexblock->returnOffset();
                node->nextSibling = right_node->returnOffset();
                right_node->parent = indexblock->returnOffset();
                right_node->nextSibling = -1;//这句好像不用，加上以防万一
            }else{
                //更新parent和next
                right_node->parent = node->parent;
                right_node->nextSibling = node->nextSibling;
                node->nextSibling = right_node->returnOffset();
            }
            //更新父节点（根节点）
            IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
            //因为是二级索引，所以这里直接插入，不进行更新，超出MAX_keyNum直接报错
            int tmp;
            int tmp_value = index_value;//用来更新数组的工具值
            //索引节点需要同时更新ptrs[]
            int p_tmp;
            int p_tmp_value = p_index_value;//之前保留的node的offset
            for(int i = 0;i < parent->keyNum;i++){
                if(parent->keyi[i] > index_value){
                    tmp = parent->keyi[i];
                    parent->keyi[i] = tmp_value;
                    tmp_value = tmp;
                    p_tmp = parent->ptrs[i];
                    parent->ptrs[i] = p_tmp_value;
                    p_tmp_value = p_tmp;
                }
            }
            parent->keyi[parent->keyNum] = tmp_value;
            parent->ptrs[parent->keyNum] = p_tmp_value;
            parent->keyNum++;
            //超出根节点最大限度
            if(parent->keyNum > MAX_keyNum){
                rootFullWarning("IndexManager::insert_node");
                return -1;
            }
            return 1;
        }
        if(TYPE == 2){
            //同type 1
            IndexBlock* right_node = BufferManager::returnFreeIndexBlock(tableName, attriName);
            int j = 0;
            float index_value = node->keyf[LEFT_keyNum];
            int p_index_value = right_node->returnOffset();
            for(int i = 0;i < MAX_keyNum;i++){
                if(i >= LEFT_keyNum){
                    right_node->keyf[j] = node->keyf[i];
                    node->keyf[i] = 0;
                    j++;
                    node->keyNum--;
                    right_node->keyNum++;
                }
            }
            if(node->parent == -1){
                IndexBlock* indexblock = BufferManager::returnFreeIndexBlock(tableName, attriName);
                indexblock->setRoot();
                indexblock->parent = -1;
                indexblock->nextSibling = 0;
                indexblock->keyNum = 1;
                indexblock->keyf[0] = index_value;
                indexblock->ptrs[0] = p_index_value;
                node->parent = indexblock->returnOffset();
                node->nextSibling = right_node->returnOffset();
                right_node->parent = indexblock->returnOffset();
                right_node->nextSibling = -1;
            }else{
                right_node->parent = node->parent;
                right_node->nextSibling = node->nextSibling;
                node->nextSibling = right_node->returnOffset();
            }
            IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
            float tmp;
            float tmp_value = index_value;
            int p_tmp;
            int p_tmp_value = p_index_value;
            for(int i = 0;i < parent->keyNum;i++){
                if(parent->keyf[i] > index_value){
                    tmp = parent->keyf[i];
                    parent->keyf[i] = tmp_value;
                    tmp_value = tmp;
                    p_tmp = parent->ptrs[i];
                    parent->ptrs[i] = p_tmp_value;
                    p_tmp_value = p_tmp;
                }
            }
            parent->keyf[parent->keyNum] = tmp_value;
            parent->ptrs[parent->keyNum] = p_tmp_value;
            parent->keyNum++;
            if(parent->keyNum > MAX_keyNum){
                rootFullWarning("IndexManager::insert_node");
                return -1;
            }
            return 1;
        }
        if(TYPE == 3){
            //同type 1
            IndexBlock* right_node = BufferManager::returnFreeIndexBlock(tableName, attriName);
            int j = 0;
            char* index_value = new char[node->KEY_LEN];
            strcpy(index_value, node->keystr[LEFT_keyNum]);
            int p_index_value = right_node->returnOffset();//父节点的对应offset应指向当前node
            for(int i = 0;i < MAX_keyNum;i++){
                if(i >= LEFT_keyNum){
                    strcpy(right_node->keystr[j], node->keystr[i]);
                    strcpy(node->keystr[i], "");
                    j++;
                    //更新keyNum
                    node->keyNum--;
                    right_node->keyNum++;
                }
            }
            if(node->parent == -1){
                //若原先是一级索引，添加根节点成为二级索引
                IndexBlock* indexblock = BufferManager::returnFreeIndexBlock(tableName, attriName);
                indexblock->setRoot();//设为根
                indexblock->parent = -1;
                indexblock->nextSibling = 0;
                indexblock->keyNum = 1;
                strcpy(indexblock->keystr[0], index_value);
                indexblock->ptrs[0] = p_index_value;
                //更新两个子节点
                node->parent = indexblock->returnOffset();
                node->nextSibling = right_node->returnOffset();
                right_node->parent = indexblock->returnOffset();
                right_node->nextSibling = -1;//这句好像不用，加上以防万一
            }else{
                //更新parent和next
                right_node->parent = node->parent;
                right_node->nextSibling = node->nextSibling;
                node->nextSibling = right_node->returnOffset();
            }
            //更新父节点（根节点）
            IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
            //因为是二级索引，所以这里直接插入，不进行更新，超出MAX_keyNum直接报错
            char* tmp = new char[node->KEY_LEN];
            char* tmp_value = new char[node->KEY_LEN];
            strcpy(tmp_value, index_value);//用来更新数组的工具值
            //索引节点需要同时更新ptrs[]
            int p_tmp;
            int  p_tmp_value = p_index_value;
            for(int i = 0;i < parent->keyNum;i++){
                if(strcmp(parent->keystr[i], index_value) > 0){
                    strcpy(tmp, parent->keystr[i]);
                    strcpy(parent->keystr[i], tmp_value);
                    strcpy(tmp_value, tmp);
                    p_tmp = parent->ptrs[i];
                    parent->ptrs[i] = p_tmp_value;
                    p_tmp_value = p_tmp;
                }
            }
            strcpy(parent->keystr[parent->keyNum], tmp_value);
            parent->ptrs[parent->keyNum] = p_tmp_value;
            parent->keyNum++;
            //超出根节点最大限度
            if(parent->keyNum > MAX_keyNum){
                rootFullWarning("IndexManager::insert_node");
                return -1;
            }
            delete[] index_value;
            delete[] tmp;
            delete[] tmp_value;
            return 1;
        }
    }
    delete[] valuestr;
    return 1;//可能有没写return 1的分支，总结写一个以防万一
}

//删除
//输入：表名，属性名，key值
//返回：-1没找到能删的或者错误
int delete_node(string tableName, string attriName, void* value){
    //查找
    int offset;
    int find_i = search(tableName, attriName, value, offset);
    int TYPE = returnIndexType(tableName, attriName);
    if(find_i == -1){
        //没找到删不了
        keyNotFoundWarning("IndexManager::delete_node");
    }else{ 
        //找到了就删，然后检测MIN_keyNum，因为是二级索引，直接设为1
        IndexBlock* node = BufferManager::returnIndexBlock(tableName, attriName, offset);
        if(node->parent == -1){
            //单根节点
            IndexBlock* parent = node;
            if(TYPE == 1){
                //直接删掉节点，更新根节点
                //没找到怎么删，先放着
                //更新根节点
                int valuei;
                char* p = (char*)value;
                memcpy(&valuei, value, 4);
                IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
                if(parent->keyNum > 2){
                    //直接删
                    int root_find_i;
                    for(int i = 0;i < parent->keyNum;i++){
                        if(parent->keyi[i] == valuei){
                            root_find_i = i;
                        }
                        if(i >= root_find_i){
                            //用后一项代替前一项
                            parent->keyi[i] = parent->keyi[i + 1];
                        }
                    }
                    parent->keyNum--;
                }else if(parent->keyNum == 2){
                    //根节点没了，用node做根节点
                    node->setRoot();
                    node->parent = -1;
                    node->nextSibling = -1;
                }else{
                    //没数据了，那表不就没了
                    //那我这里的操作应该是删掉root，先放着
                }
            }
            if(TYPE == 2){
                //同type 1
                float valuef;
                char* p = (char*)value;
                memcpy(&valuef, value, 4);
                IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
                if(parent->keyNum > 2){
                    int root_find_i;
                    for(int i = 0;i < parent->keyNum;i++){
                        if(parent->keyf[i] == valuef){
                            root_find_i = i;
                        }
                        if(i >= root_find_i){
                            parent->keyf[i] = parent->keyf[i + 1];
                        }
                    }
                    parent->keyNum--;
                }else if(parent->keyNum == 2){
                    node->setRoot();
                    node->parent = -1;
                    node->nextSibling = -1;
                }else{
                }
            }
            if(TYPE == 3){
                //同type 1
                char* valuestr = new char[node->KEY_LEN];
                char* p = (char*)value;
                memcpy(&valuestr, value, node->KEY_LEN);
                IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
                if(parent->keyNum > 2){
                    int root_find_i;
                    for(int i = 0;i < parent->keyNum;i++){
                        if(strcmp(parent->keystr[i], valuestr) == 0){
                            root_find_i = i;
                        }
                        if(i >= root_find_i){
                            strcpy(parent->keystr[i], parent->keystr[i + 1]);
                        }
                    }
                    parent->keyNum--;
                }else if(parent->keyNum == 2){
                    node->setRoot();
                    node->parent = -1;
                    node->nextSibling = -1;
                }else{
                }
                delete[] valuestr;
            }
        }
        if(node->keyNum > MIN_keyNum){
            //直接删，有可能删到第一项，要更新根节点索引
            if(TYPE == 1){
                int valuei;
                char* p = (char*)value;
                memcpy(&valuei, value, 4);
                for(int i = 0;i < node->keyNum;i++){
                    if(i >= find_i){
                        //用后一项代替
                        node->keyi[i] = node->keyi[i + 1];
                    }
                }
                node->keyNum--;
                if(find_i == 0){
                    //第一项，更新根节点，用当前该节点的第一个值替换
                    IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
                    for(int i = 0;i < parent->keyNum;i++){
                        if(parent->keyi[i] == valuei){
                            parent->keyi[i] = node->keyi[0];
                        }
                    }
                }
            }
            if(TYPE == 2){
                //同type 1
                float valuef;
                char* p = (char*)value;
                memcpy(&valuef, value, 4);
                for(int i = 0;i < node->keyNum;i++){
                    if(i >= find_i){
                        node->keyf[i] = node->keyf[i + 1];
                    }
                }
                node->keyNum--;
                if(find_i == 0){
                    IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
                    for(int i = 0;i < parent->keyNum;i++){
                        if(parent->keyf[i] == valuef){
                            parent->keyf[i] = node->keyf[0];
                        }
                    }
                }
            }
            if(TYPE == 3){
                //同type 1
                char* valuestr = new char[node->KEY_LEN];
                char* p = (char*)value;
                memcpy(&valuestr, value, node->KEY_LEN);
                for(int i = 0;i < node->keyNum;i++){
                    if(i >= find_i){
                        //用后一项代替
                        strcpy(node->keystr[i], node->keystr[i + 1]);
                    }
                }
                node->keyNum--;
                if(find_i == 0){
                    //第一项，更新根节点，用当前该节点的第一个值替换
                    IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
                    for(int i = 0;i < parent->keyNum;i++){
                        if(strcmp(parent->keystr[i], valuestr) == 0){
                            strcpy(parent->keystr[i], node->keystr[0]);
                        }
                    }
                }
                delete[] valuestr;
            }
        }else{//node->keyNum == MIN_keyNum == 1
            if(TYPE == 1){
                //直接删掉节点，更新根节点
                //没找到怎么删，先放着
                //更新根节点
                int valuei;
                char* p = (char*)value;
                memcpy(&valuei, value, 4);
                IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
                if(parent->keyNum > 2){
                    //直接删
                    int root_find_i;
                    for(int i = 0;i < parent->keyNum;i++){
                        if(parent->keyi[i] == valuei){
                            root_find_i = i;
                        }
                        if(i >= root_find_i){
                            //用后一项代替前一项
                            parent->keyi[i] = parent->keyi[i + 1];
                        }
                    }
                    parent->keyNum--;
                }else if(parent->keyNum == 2){
                    //根节点没了，用node做根节点
                    node->setRoot();
                    node->parent = -1;
                    node->nextSibling = -1;
                }else{
                    //没数据了，那表不就没了
                    //那我这里的操作应该是删掉root，先放着
                }
            }
            if(TYPE == 2){
                //同type 1
                float valuef;
                char* p = (char*)value;
                memcpy(&valuef, value, 4);
                IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
                if(parent->keyNum > 2){
                    int root_find_i;
                    for(int i = 0;i < parent->keyNum;i++){
                        if(parent->keyf[i] == valuef){
                            root_find_i = i;
                        }
                        if(i >= root_find_i){
                            parent->keyf[i] = parent->keyf[i + 1];
                        }
                    }
                    parent->keyNum--;
                }else if(parent->keyNum == 2){
                    node->setRoot();
                    node->parent = -1;
                    node->nextSibling = -1;
                }else{
                }
            }
            if(TYPE == 3){
                //同type 1
                char* valuestr = new char[node->KEY_LEN];
                char* p = (char*)value;
                memcpy(&valuestr, value, node->KEY_LEN);
                IndexBlock* parent = BufferManager::returnIndexBlock(tableName, attriName, node->parent);
                if(parent->keyNum > 2){
                    int root_find_i;
                    for(int i = 0;i < parent->keyNum;i++){
                        if(strcmp(parent->keystr[i], valuestr) == 0){
                            root_find_i = i;
                        }
                        if(i >= root_find_i){
                            strcpy(parent->keystr[i], parent->keystr[i + 1]);
                        }
                    }
                    parent->keyNum--;
                }else if(parent->keyNum == 2){
                    node->setRoot();
                    node->parent = -1;
                    node->nextSibling = -1;
                }else{

                }
                delete[] valuestr;
            }
        }
    }
    return 1;//可能有没写return 1的分支，总结写一个以防万一
}