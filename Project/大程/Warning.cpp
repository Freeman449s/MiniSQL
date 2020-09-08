#include "Warning.h"

void functionFailureWarning(string funName, string failureFunName) {
	cout << "Warning: Function " << failureFunName << " failure.";
	cout << " Position: " << funName << endl;
}

void tableNotExistWarning(string funName, string tableName) {
	cout << "Warning: Table \"" << tableName << "\" does not exist.";
	cout << " Position: " << funName << endl;
}

void attriNotExistWarning(string funName, string tableName, string attriName) {
	cout << "Warning: Attribute \"" << attriName << "\" of table \"" << tableName << "\" does not exist.";
	cout << " Position: " << funName << endl;
}

void fileCannotOpenWarning(string funName, string fileName) {
	cout << "Warning: File \"" << fileName << "\" cannot be opened.";
	cout << " Position: " << funName << endl;
}

void internalErrorWarning(string funName, string description) {
	cout << "Warning: Internal Error: " << description;
	cout << " Position: " << funName << endl;
}

void offsetOutOfRangeWarning(string funName) {
	cout << "Warning: Offset out of range.";
	cout << " Position: " << funName << endl;
}

void blockPinnedWarning(string funName, string fileName) {
	cout << "Warning: Unable to free a block of file \"" << fileName << "\" because block is pinned.";
	cout << " Position: " << funName << endl;
}

void invalidIDWarning(string funName) {
	cout << "Warning: Invalid ID received. Position: " << funName << endl;
}

void attriNotIndexedWarning(string funName, string tableName, string attriName) {
	cout << "Attribute \"" << attriName << "\" of table \"" << tableName << "\" is not indexed"
		<< " or does not exist. Position: " << funName << endl;
}

// ========================= Index Manager 加入的警告 ====================================

void attriNotUniqueWarning(string funName) {
	cout << "Warning: Attribute not unique. Position: " << funName << endl;
}

void keyDuplicateWarning(string funName) {
	cout << "Warning: Key inserting already exist. Position: " << funName << endl;
}

void rootFullWarning(string funName) {
	cout << "Warning: Root is full, cannot be inserted. Position: " << funName << endl;
}

void keyNotFoundWarning(string funName) {
	cout << "Warning: Key deleting not found. Position: " << funName << endl;
}

// ======================================================================================