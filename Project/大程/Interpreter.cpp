#include "Interpreter.h"
#include "CatalogManager.h"
#include "recordManager.h"
//执行interpreter
void use_interpreter()
{
	cout << "\n                    ---------------------------------------------\n";
	cout << "                                   欢迎使用minisql\n";
	cout << "                    ---------------------------------------------\n\n\n\n";
	while (1) {
		string user_input;
		cout << "等待用户输入指令：";
		if (get_input(user_input)) {
			if (!check_type(user_input)) cout << error_info << endl;
		}
		else cout << error_info << endl;
	}
}

//测试专用
int main() {
	read_dict_file();
	use_interpreter();
}

//得到用户输入的数据并处理成一条字符串
bool get_input(string& user_input) {
	string cin_string;
	while (true) {
		cin >> cin_string;
		if (user_input.length() != 0)
			cin_string = " " + cin_string;
		user_input.append(cin_string);
		int pos = user_input.find(";");//记录读取结束
		if (pos != -1)
		{
			//	如果结束标记符后面还有输入，则报错
			if (pos != user_input.length() - 1)
			{
				error_info = ERROR_WRONG_INPUT;
				return false;
			}
			else
				return true;
		}
	}
}


//此函数用于判断用户输入指令的类型并移交其他函数处理该命令
bool check_type(string user_input) {
	string type = seperate_word(user_input, 1);
	delete_first_space(type);
	if (type == "create") {
		int attr_num;
		if (!check_create(user_input, attr_num)) {
			return false;
		}
		/*else if (translated_record_type == CREATE_DATABASE) {
			//return create_database(translated_record);
		}*/
		else if (translated_record_type == CREATE_TABLE) {
			if (create_table(translated_record, attr_num)) {
				cout << "create table成功！" << endl;
				return true;
			}
			else {
				error_info = ERROR_MAKING_CREATE_TABLE;
				return false;
			}
		}
		else if (translated_record_type == CREATE_INDEX) {
			if (create_index(translated_record)) {
				cout << "create index成功！" << endl;
				return true;
			}
			else {
				error_info = ERROR_MAKING_CREATE_INDEX;
				return false;
			}
		}
	}
	else if (type == "delete") {
		string result = check_delete(user_input);
		if (result != "false") {
			if (delete_record_true(result)) {
				delete_record(result);
				cout << "删除命令已执行！" << endl;
			}
			else {
				error_info = ERROR_USELESS_DELETE;
				return false;
			}
		}
		else {
			error_info = ERROR_USELESS_DELETE;
			return false;
		}
	}
	else if (type == "drop") {
		string result = check_drop(user_input);
		if (result != "false") {
			if (translated_record_type == DROP_DATABASE) {
				//drop_database(result);
			}
			else if (translated_record_type == DROP_TABLE) {
				if (drop_table(result)) {
					cout << "删除成功！" << endl;
					return true;
				}
				else {
					error_info = ERROR_TABLE_NOT_EXIST;
					return false;
				}
			}
			else if (translated_record_type == DROP_INDEX) {
				if (drop_index(result)) {
					cout << "删除成功！" << endl;
					return true;
				}
				else {
					error_info = ERROR_INDEX_NOT_EXIST;
					return false;
				}
			}
		}
		else {
			error_info = ERROR_USELESS_DROP;
			return false;
		}
	}
	else if (type == "insert") {
		string result = check_insert(user_input);
		if (result != "false") {
			if (insert_record_true(result)) {
				if (insert_record(result)) cout << "数据插入成功！" << endl;
				else cout << "不允许在Unique属性上插入重复数据。" << endl;
			}
			else {
				error_info = ERROR_INSERT_WRONG;
				return false;
			}
		}
		else {
			error_info = ERROR_USELESS_INSERT;
			return false;
		}
	}
	else if (type == "select") {
		string result = check_select(user_input);
		if (result != "false") {
			if (select_record_true(result)) {
				cout << "下面为您输出数据：" << endl;
				select_record(result);
			}
			else {
				error_info = ERROR_USELESS_SELECT;
				return false;
			}
		}
		else {
			error_info = ERROR_USELESS_SELECT;
			return false;
		}
	}
	else if (type == "execfile") {
		string result = seperate_word(user_input, 2);
		if (result[result.length() - 1] != ';' && seperate_word(user_input, 3) != ";")return false;
		open_execfile(result);
	}
	else if (user_input == "quit ;" || type == "quit;") {
		BufferManager::shutdown();
		cout << endl;
		cout << "                    ---------------------------------------------" << endl;
		cout << "                                    感谢您的使用！                    " << endl;
		cout << "                    ---------------------------------------------" << endl;
		cout << endl;
		exit(-1);
	}
	else {
		error_info = ERROR_USELESS_INPUT;
		return false;
	};
}


//检查create语句
bool check_create(string user_input, int& attr_num) {
	string type = seperate_word(user_input, 2);
	if (type == "table") {
		translated_record = check_create_table(user_input, attr_num);
		translated_record_type = CREATE_TABLE;
		if ("false" != translated_record && attr_num != 0) return true;
		else {
			error_info = ERROE_CREATE_TABLE;
			return false;
		}
	}
	else if (type == "index") {
		translated_record = check_create_index(user_input);
		translated_record_type = CREATE_INDEX;
		if ("false" != translated_record) return true;
		else {
			error_info = ERROE_CREATE_INDEX;
			return false;
		}
	}
	/*else if (type == "database") {
		translated_record = check_create_database(user_input);
		translated_record_type = CREATE_DATABASE;
		if ("false" != translated_record) return true;
		else return false;
	}*/
	else {
		error_info = ERROR_USELESS_CREATE;
		return false;
	}
}

//返回值为：数据库名
string check_create_database(string user_input) {
	string database_name = seperate_word(user_input, 3);
	int name_place = user_input.find(database_name);
	if (database_name == "," || database_name == "(" || database_name == ")" || database_name == ";")
	{
		return "false";
	}
	name_place += database_name.length();
	if (!(user_input[name_place] == ';' || (user_input[name_place] == ' ' && user_input[name_place + 1] == ';'))) {
		return "false";
	}
	delete_last_space(database_name);
	return database_name;
}
//返回值为：索引名 表名 属性名
string check_create_index(string user_input) {
	string output = "";
	string index_name = seperate_word(user_input, 3);
	if (index_name == "," || index_name == "(" || index_name == ")" || index_name == ";")
	{
		return "false";
	}
	if (seperate_word(user_input, 4) != "on")
	{
		return "false";
	}
	bool is_begin = true;//判断是否为开头的空格
	string table_name = "";
	int name_begin;
	for (name_begin = user_input.find("on") + 2; name_begin < user_input.length(); name_begin++) {
		if (!(user_input[name_begin] == ' ' && is_begin == true)) {
			is_begin = false;
			if (user_input[name_begin] != '(')
				table_name = table_name + user_input[name_begin];
			else break;
		}
	}
	delete_last_space(table_name);
	if (name_begin == user_input.length() || table_name == "" || table_name.find(' ') != -1) return "false";
	string attr_name;
	for (name_begin = name_begin + 1; name_begin < user_input.length(); name_begin++) {
		if (user_input[name_begin] != ')') {
			attr_name = attr_name + user_input[name_begin];
		}
		else break;
	}
	if (!(user_input[name_begin + 1] == ';' || (user_input[name_begin + 1] == ' ' && user_input[name_begin + 2] == ';'))) {
		return "false";
	}
	output = output + " " + index_name + " " + table_name + " " + attr_name;
	if (attr_name == "") return "false";
	return output;
}
/*
create index sdafsf on box (dfa) ;
*/
//返回值为：表名 属性名 类型 属性名 类型 ······ 主键
string check_create_table(string user_input, int& attr_num) {
	string output = "";
	string table_name = seperate_word(user_input, 3);
	if (table_name == "," || table_name == "(" || table_name == ")" || table_name == ";")
	{
		return "false";
	}
	output = table_name;
	if (seperate_word(user_input, 4) != "(")
	{
		return "false";
	}
	int attribute_num = 0;
	string temp_record = "";
	string result = "";//存储primary key
	user_input.insert(user_input.length() - 3, ",");
	for (int iter_begin = user_input.find('(') + 1; iter_begin < user_input.length(); iter_begin++) {
		if (user_input[iter_begin] == ',') {
			attribute_num++;
			string attr_type = seperate_word(temp_record, 2);
			delete_last_space(temp_record);
			//如果是char类型的属性
			if (attr_type.find("char") != -1) {
				if (attr_type == "char") {
					bool check_unique_exist = false;
					if (check_unique(temp_record)) check_unique_exist = true;
					string char_num = seperate_word(temp_record, 3);
					if (char_num[0] == '(') char_num = char_num.substr(1);
					else return "false";
					if (char_num[char_num.length() - 1] == ')') char_num = char_num.substr(0, char_num.length() - 1);
					else return "false";
					output = output + ' ' + seperate_word(temp_record, 1) + " char " + char_num;
					if (check_unique_exist) output = output + " unique";
				}
				else {
					bool check_unique_exist = false;
					if (check_unique(temp_record)) check_unique_exist = true;
					if (attr_type.find('(') != 4) return "false";
					int temp_iter = attr_type.find('(') + 1;
					string char_num = "";
					if (attr_type.find(')') != attr_type.length() - 1) return "false";
					while (temp_iter < attr_type.length() - 1) {
						if (attr_type[temp_iter] >= '0' && attr_type[temp_iter] <= '9') {
							char_num = char_num + attr_type[temp_iter];
						}
						else return "false";
						temp_iter++;
					}
					output = output + ' ' + seperate_word(temp_record, 1) + " char " + char_num;
					if (check_unique_exist) output = output + " unique";
				}
			}
			//如果是int类型的属性
			else if (attr_type == "int") {
				bool check_unique_exist = false;
				if (check_unique(temp_record)) check_unique_exist = true;
				if (temp_record.find("int") != temp_record.length() - 3) return "false";
				output = output + ' ' + seperate_word(temp_record, 1) + " int";
				if (check_unique_exist) output = output + " unique";
			}
			//如果是float类型的属性
			else if (attr_type == "float") {
				bool check_unique_exist = false;
				if (check_unique(temp_record)) check_unique_exist = true;
				if (temp_record.find("float") != temp_record.length() - 5) return "false";
				output = output + ' ' + seperate_word(temp_record, 1) + " float";
				if (check_unique_exist) output = output + " unique";
			}
			else if (seperate_word(temp_record, 1) == "primary" || seperate_word(temp_record, 1) == " primary") {
				if (seperate_word(temp_record, 2).find("key") != -1) {
					attribute_num--;
					int key_num = temp_record.find("key") + 3;
					if (seperate_word(temp_record, 2).find("key") == -1) return "false";
					if (temp_record[key_num] == ' ') key_num++;
					if (temp_record[key_num] != '(') return "false";
					if (temp_record[temp_record.length() - 1] != ')')return "false";
					result = temp_record.substr(key_num + 1, temp_record.length() - key_num - 2);
				}
				else return "false";
			}
			else return "false";
			temp_record = "";
		}
		else temp_record = temp_record + user_input[iter_begin];
	}
	attr_num = attribute_num;
	if (attr_num > 32) return "false";
	if (result != "") {
		output = output + " " + result + " ;";
	}
	return output;
}

//返回值为：表名 ;     或      表名 符号 属性名 属性值 （与或 符号 属性名 属性值）;
//例： table > a 3 and < dsf 4 ;
//例： table ;
string check_delete(string user_input) {
	string output = "";
	if (seperate_word(user_input, 2) != "from") {
		return "false";
	}
	if (seperate_word(user_input, 4) == ";")
	{
		output = seperate_word(user_input, 3);
		output = output + " ;";
		return output;
	}
	else if (seperate_word(user_input, 3).find(";") != -1) {
		output = seperate_word(user_input, 3);
		output = output.substr(0, output.length() - 1);
		output = output + " ;";
		return output;
	}
	else if (seperate_word(user_input, 4) == "where") {
		output = seperate_word(user_input, 3) + " ";
		int i = 5;
		int and_or_type = 0;//0表示没有and或or条件，1表示and条件，2表示or条件
		int con1_begin, con1_end = 0, con2_begin, con2_end;//条件的在user_input中的起始位置和结束位置
		while (1) {
			if (seperate_word(user_input, i).find(';') != -1)
				break;
			if (seperate_word(user_input, i) == "and") {
				con1_end = user_input.find("and") - 1;
				con2_begin = user_input.find("and") + 3;
				and_or_type = 1;
				break;
			}
			if (seperate_word(user_input, i) == "or") {
				con1_end = user_input.find("or") - 1;
				con2_begin = user_input.find("or") + 2;
				and_or_type = 2;
				break;
			}
			if (con1_end == 0) {
				con1_end = user_input.length() - 2;
			}
			i++;
		}
		con1_begin = user_input.find("where") + 5;
		con2_end = user_input.find(";") - 1;
		if (and_or_type == 0) {
			string temp_string = user_input.substr(con1_begin, con1_end - con1_begin + 1);//截取第一个条件
			string ans = check_delete_type(temp_string);
			if (ans == "false") return "false";
			else output = output + ans;
		}
		else if (and_or_type == 1) {
			string temp_string = user_input.substr(con1_begin, con1_end - con1_begin + 1);//截取第一个条件
			string ans = check_delete_type(temp_string);
			if (ans == "false") return "false";
			else output = output + ans + "and ";
			temp_string = user_input.substr(con2_begin, con2_end - con2_begin + 1);//截取第二个条件
			ans = check_delete_type(temp_string);
			if (ans == "false") return "false";
			else output = output + ans + " ;";
		}
		else {
			string temp_string = user_input.substr(con1_begin, con1_end - con1_begin + 1);//截取第一个条件
			string ans = check_delete_type(temp_string);
			if (ans == "false") return "false";
			else output = output + ans + "or ";
			temp_string = user_input.substr(con2_begin, con2_end - con2_begin + 1);//截取第二个条件
			ans = check_delete_type(temp_string);
			if (ans == "false") return "false";
			else output = output + ans + " ;";
		}
	}
	else return "false";
	return output;
}
/* delete from table where a > b and c>d;*/

//返回值为： 名字 ;
string check_drop(string user_input) {
	string type = seperate_word(user_input, 2);
	string output = "";
	int last = user_input.find(";");
	if (type == "table") {
		translated_record_type = DROP_TABLE;
		string name = user_input.substr(10, last - 10);
		delete_first_space(name);
		delete_last_space(name);
		if (name.find(" ") != -1) return "false";
		else output = name;
	}
	else if (type == "index") {
		translated_record_type = DROP_INDEX;
		string name = user_input.substr(10, last - 10);
		delete_first_space(name);
		delete_last_space(name);
		if (name.find(" ") != -1) return "false";
		else output = name;
	}
	else if (type == "database") {
		translated_record_type = DROP_DATABASE;
		string name = user_input.substr(13, last - 13);
		delete_first_space(name);
		delete_last_space(name);
		if (name.find(" ") != -1) return "false";
		else output = name;
	}
	else return "false";
	return output;
}

string check_insert(string user_input) {
	string output = "";
	if (seperate_word(user_input, 2) != "into") return "false";
	string table_name = seperate_word(user_input, 3);
	output = table_name + " ";
	int iter = user_input.find("values");
	if (iter == -1) return "false";
	else if (user_input[iter + 6] == '(' || (user_input[iter + 6] == ' ' && user_input[iter + 7] == '(')) {
		iter = user_input.find("(");
	}
	else return "false";
	int i_begin = iter + 1;//记录属性的开头位置
	for (int i_end = iter + 1; i_end < user_input.length(); i_end++) {
		if (user_input[i_end] == ',') {
			string temp = user_input.substr(i_begin, i_end - i_begin);
			delete_first_space(temp);
			delete_last_space(temp);
			if (temp.find(' ') != -1) return "false";
			else output = output + temp + " ";
			i_begin = i_end + 1;
		}
		else if (user_input[i_end] == ')') {
			string temp = user_input.substr(i_begin, i_end - i_begin);
			delete_first_space(temp);
			delete_last_space(temp);
			if (temp.find(' ') != -1) return "false";
			else output = output + temp + " ";
			if (!(user_input[i_end + 1] == ';' || (user_input[i_end + 1] == ' ' && user_input[i_end + 2] != ';'))) return "false";
			break;
		}
	}
	output = output + "; ";
	return output;
}
/* insert into table values(asd,adf,sd ,sdaf,sdfa);*/

string check_delete_type(string input) {
	string output = "";
	int i = 0;
	bool found = false;
	string a;
	string b;
	for (; i < input.length(); i++) {
		if (input.substr(i, 1) == "<") {
			if (input.substr(i + 1, 1) == "=") {
				output = "<= ";
				i++;
				a = input.substr(0, i - 1);
				b = input.substr(i + 1, input.length() - i - 1);
			}
			else if (input.substr(i + 1, 1) == ">") {
				output = "<> ";
				i++;
				a = input.substr(0, i - 1);
				b = input.substr(i + 1, input.length() - i - 1);
			}
			else {
				output = "< ";
				a = input.substr(0, i);
				b = input.substr(i + 1, input.length() - i - 1);
			}
			if (found == false)
				found = true;
			else return "false";
		}
		else if (input.substr(i, 1) == ">") {
			if (input.substr(i + 1, 1) == "=") {
				output = ">= ";
				i++;
				a = input.substr(0, i - 1);
				b = input.substr(i + 1, input.length() - i - 1);
			}
			else {
				output = "> ";
				a = input.substr(0, i);
				b = input.substr(i + 1, input.length() - i - 1);
			}
			if (found == false)
				found = true;
			else return "false";
		}
		else if (input.substr(i, 1) == "=") {
			output = "= ";
			if (found == false)
				found = true;
			else return "false";
			a = input.substr(0, i);
			b = input.substr(i + 1, input.length() - i - 1);
		}
	}
	if (found == false) return "false";
	delete_last_space(a);
	delete_first_space(a);
	delete_last_space(b);
	delete_first_space(b);
	if (a.find(' ') != -1 || b.find(' ') != -1)return "false";
	output = output + a + " " + b + " ";
	return output;
}

//返回值为：表名 
string check_select(string user_input) {
	if (seperate_word(user_input, 2) != "*") {
		return "false";
	}
	if (seperate_word(user_input, 3) != "from") {
		return "false";
	}
	if (seperate_word(user_input, 4).find(';') != -1) {
		string output = seperate_word(user_input, 4);
		output = output.substr(0, output.length() - 1);
		output = output + " ;";
		return output;
	}
	if (seperate_word(user_input, 5) != "where") {
		return "false";
	}
	string output = seperate_word(user_input, 4) + " ";
	int i = 5;
	int and_or_type = 0;//0表示没有and或or条件，1表示and条件，2表示or条件
	int con1_begin, con1_end, con2_begin, con2_end;//条件的在user_input中的起始位置和结束位置
	bool have_found = false;
	while (1) {
		if (seperate_word(user_input, i).find(';') != -1)
			break;
		if (seperate_word(user_input, i) == "and") {
			have_found = true;
			con1_end = user_input.find("and") - 1;
			con2_begin = user_input.find("and") + 3;
			and_or_type = 1;
			break;
		}
		if (seperate_word(user_input, i) == "or") {
			have_found = true;
			con1_end = user_input.find("or") - 1;
			con2_begin = user_input.find("or") + 2;
			and_or_type = 2;
			break;
		}
		i++;
	}
	if (have_found == false) con1_end = user_input.length() - 2;
	con1_begin = user_input.find("where") + 5;
	con2_end = user_input.find(";") - 1;
	if (and_or_type == 0) {
		string temp_string = user_input.substr(con1_begin, con1_end - con1_begin + 1);//截取第一个条件
		string ans = check_delete_type(temp_string);
		if (ans == "false") return "false";
		else output = output + ans;
	}
	else if (and_or_type == 1) {
		string temp_string = user_input.substr(con1_begin, con1_end - con1_begin + 1);//截取第一个条件
		string ans = check_delete_type(temp_string);
		if (ans == "false") return "false";
		else output = output + ans + "and ";
		temp_string = user_input.substr(con2_begin, con2_end - con2_begin + 1);//截取第二个条件
		ans = check_delete_type(temp_string);
		if (ans == "false") return "false";
		else output = output + ans + " ;";
	}
	else {
		string temp_string = user_input.substr(con1_begin, con1_end - con1_begin + 1);//截取第一个条件
		string ans = check_delete_type(temp_string);
		if (ans == "false") return "false";
		else output = output + ans + "or ";
		temp_string = user_input.substr(con2_begin, con2_end - con2_begin + 1);//截取第二个条件
		ans = check_delete_type(temp_string);
		if (ans == "false") return "false";
		else output = output + ans + " ;";
	}
	return output;
}
/* select * from t1 where name ='b' and salary<>3.0; */


//检查是否为unique类型
bool check_unique(string& string_input) {
	if (string_input.find("unique") == string_input.length() - 6) {
		string_input = string_input.substr(0, string_input.length() - 6);
		delete_last_space(string_input);
		return true;
	}
	else return false;
}

bool open_execfile(string& file_input)
{
	if (file_input[file_input.length() - 1] == ';') {
		file_input = file_input.substr(0, file_input.length() - 1);
	}
	string user_input = "";
	char input[65536];
	ifstream fcin;
	fcin.open(file_input);
	if (!fcin)
	{
		error_info = ERROR_FILE_NOT_EXIST;
		return false;
	}
	while (!fcin.eof())
	{
		fcin.getline(input, 65536);
		user_input = user_input + " " + input;
		if (user_input.find(";") != -1) {
			if (!check_type(user_input)) cout << error_info << endl;
			user_input = "";
		}
	}
	fcin.close();
}

/*
create table sample (
a char(8) unique  ,
b char(16) unique,
c int unique,
d int
);

*/