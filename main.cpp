
/*-------------------------------------------------------
External Sort Assignment
Date submission - April 8th, 2019
Author: Nguyen Hai Chau
---------------------------------------------------------*/

#include<bits/stdc++.h>
#include <pthread.h>
using namespace std;




/*-----------------------------------------------------------------------------------
Function to create small sorted files from input file using merge sort
Taking 3 inputs:
1. Name of the input file
2. Maximum size a thread can be
3. Maximum number of threads can be spawned
-------------------------------------------------------------------------------------*/
void mainFileIntoSortedFiles(string nameInputFile, unsigned long long threadSize, long long maximumThreads);



/*-----------------------------------------------------------------------------------
Function to sort a vector of lines, this function will be called
by different threads as each thread will sort a different segment
of the original file and directly write them to hard drive as
output files, the name of the output files will be matched with
thread number.
For example:
thread 0 will sort then write to 0.txt
thread 1 will sort then write to 1.txt
Taking 2 inputs:
1. Vector of lines - to be sorted
2. Thread number - number of the thread, which will determine
the name of the output file
-------------------------------------------------------------------------------------*/
void SortLines(vector<string> lines, unsigned int threadNumber);





/*-----------------------------------------------------------------------------------
Those 2 functions are implementation of a standard merge sort
for strings, a REFERENCE of the vector of lines will be passed as
input and will be sorted in-place, therefore they do not return
anything.
void mergeSort taking 3 inputs:
1. vector of lines
2. left boundary
3. right boundary
void merge taking 5 inputs:
1. vector of lines
2. left boundary of the first half
3. right boundary of the first half
4. left boundary of the second half
5. right boundary of the second half
-------------------------------------------------------------------------------------*/
void mergeSort(vector<string>& lines, unsigned int left, unsigned int right);
void merge(vector<string>& lines, unsigned int left_1, unsigned int right_1, unsigned int left_2, unsigned int right_2);


/*-----------------------------------------------------------------------------------
Function to combine 2 sorted files into 1 using merge sort
Taking 2 inputs:
1. File name counter - this argument will determine what
are the two files which will be combined, for example,
if counter = 0 then 0.txt and 1.txt will be merged
if counter = 1 then 2.txt and 3.txt will be merged
2. Bool underScore
-------------------------------------------------------------------------------------*/

void mergeTwoSortedFiles(int fileNameCounter, bool underScore);


/*-----------------------------------------------------------------------------------
Using divide and conquer, merge 2 sorted files into one.
For example, initially we have 8 small sorted files.
[0] [1] [2] [3] [4] [5] [6] [7]
after the first iteration it will be 4 sorted files, as 0, 1
will be merged into 0; 2, 3 will be merged into 1 and so on
[0] [1] [2] [3]
in the end, we will have a single sorted file [0] as our result
-------------------------------------------------------------------------------------*/
void mergeKSortedFiles(bool underScore, long long maximumThreads);




/*-----------------------------------------------------------------------------------
Function return the size of the file.
-------------------------------------------------------------------------------------*/
ifstream::pos_type filesize(const char* filename);



/*-----------------------------------------------------------------------------------
Global variavle total output files,
Initially, after the original file is sliced into K small
files, it will be initialized to K. Then K will be merge into
K / 2, then K / 4. Once the variable == 1. The recursive functions
will be terminated.
-------------------------------------------------------------------------------------*/
unsigned totalOutputFiles = 0;


int main(int argc, char ** argv){
	string in = argv[1];
	string ou = argv[2];
	long long MAXIMUM_THREADS = 100;
	auto start = chrono::high_resolution_clock::now();
//	ios_base::sync_with_stdio (false);
	cout << "Running" << endl;
	////////////////////////////////////////////////////////////////////
    unsigned long long memorySize = stoi(argv[3]);
	if (argc > 4) {
		memorySize = memorySize * stoi(argv[4]) / 100;
	}
    unsigned long long fileSize = filesize(in.c_str());
    if (fileSize < memorySize) {
    	MAXIMUM_THREADS = 1;
    }

	unsigned long long threadSize  = 1; // memorySize / MAXIMUM_THREADS;


    mainFileIntoSortedFiles(in, threadSize, MAXIMUM_THREADS);
	bool underScore = false;

	mergeKSortedFiles(underScore, MAXIMUM_THREADS);

	ifstream output("0.txt");
	if (output.good()){
		rename("0.txt", ou.c_str());
	}
	else{
		rename("0_.txt", ou.c_str());
	}
	output.close();

	////////////////////////////////////////////////////////////////////
	auto finish = chrono::high_resolution_clock::now();
	chrono::duration<double> elapsed = finish - start;
	cout << "Running time: " << elapsed.count() << "\n";

	return 0;
}

void mainFileIntoSortedFiles(string input, unsigned long long threadSize, long long MAXIMUM_THREADS){
	ifstream inFile;
	inFile.open(input);
	string line;
	queue<thread> threads;
	vector<string> lines;
	unsigned long curSize = 0;
	unsigned int threadCounter = 0;

	while (getline(inFile, line)){
		unsigned long long lineSize = line.size();
		if (curSize + lineSize > threadSize) {

			if (threads.size() == MAXIMUM_THREADS) {
				threads.front().join();
				threads.pop();
			}
			threads.push(thread(SortLines, lines, threadCounter));
			threadCounter ++;

			curSize = line.size();
			lines = {line};
		}
		else {
			lines.push_back(line);
			curSize += line.size();
		}

	}

	inFile.close();
	while (!threads.empty()) {
		threads.front().join();
		threads.pop();
	}
	if (curSize != 0) {
		SortLines(lines, threadCounter);
	}
}
void mergeTwoSortedFiles(int fileNameCounter, bool underScore){
	string file1, file2;
	ofstream output(underScore == false
            ? to_string(fileNameCounter / 2) + "_.txt"
            : to_string(fileNameCounter / 2) + ".txt"
            );
	if (underScore == false) {
		file1 = (to_string(fileNameCounter) + ".txt");
		file2 = (to_string(fileNameCounter + 1) + ".txt");
	}
	else {
		file1 = (to_string(fileNameCounter) + "_.txt");
		file2 = (to_string(fileNameCounter + 1) + "_.txt");
	}
	ifstream input1(file1), input2(file2);
	string line1, line2;
	getline(input1, line1);
	getline(input2, line2);
	while (line1 != "\0" && line2 != "\0") {
		if (line1.compare(line2) < 0) {
			output << line1 << endl;
			getline(input1, line1);
		}
		else {
			output << line2 << endl;
			getline(input2, line2);
		}
		output.flush();
	}
	while (line1 != "\0") {
		output << line1 << endl;
		getline(input1, line1);
		output.flush();
	}
	while (line2 != "\0") {
		output << line2 << endl;
		getline(input2, line2);
		output.flush();
	}
	input1.close(); input2.close();
	remove(file1.c_str());
	remove(file2.c_str());
	output.close();


}


void merge(vector<string> &lines, unsigned int l1, unsigned int r1, unsigned int l2, unsigned int r2) {
	vector<string> copy;
	unsigned int ind1 = l1, ind2 = l2;

	while(ind1 < r1 && ind2 < r2) {
		if (lines[ind1].compare(lines[ind2]) < 0) {
			copy.push_back(lines[ind1]);
			ind1 ++;
		}
		else {
			copy.push_back(lines[ind2]);
			ind2 ++;
		}
	}
	for (unsigned int i = ind1; i < r1; i ++) {
		copy.push_back(lines[i]);
	}
	for (unsigned int i = ind2; i < r2; i ++) {
		copy.push_back(lines[i]);
	}
	for (unsigned int i = l1; i < r2; i ++) {
		lines[i] = copy[i - l1];
	}
	return;
}

void mergeSort(vector<string> &lines, unsigned int l, unsigned int r){
	if (l >= r || l == lines.size() - 1) {
		return;
	}
	else if(l + 1 == r) {
		if (lines[l].compare(lines[r]) > 0){
			swap(lines[l], lines[r]);
		}
		return;
	}
	int m = (l + r) / 2;
	mergeSort(lines, l, m);
	mergeSort(lines, m, r);
	merge(lines, l, m, m, r);

}

void mergeKSortedFiles(bool underScore, long long MAXIMUM_THREADS) {
	queue<thread> threads;

	while (totalOutputFiles > 1) {
		for(int i = 0; i < totalOutputFiles / 2; i ++) {
			if (threads.size() == MAXIMUM_THREADS) {
				threads.front().join();
				threads.pop();
			}
			threads.push(thread(mergeTwoSortedFiles, i * 2, underScore));
		}
		if (totalOutputFiles % 2 == 1 && underScore == false) {
			rename((to_string(totalOutputFiles - 1) + ".txt").c_str(), (to_string(totalOutputFiles / 2) + "_.txt").c_str());
		}
		else if (totalOutputFiles % 2 == 1 && underScore == true) {
			rename((to_string(totalOutputFiles - 1) + "_.txt").c_str(), (to_string(totalOutputFiles / 2) + ".txt").c_str());
		}
		while (!threads.empty()) {
			threads.front().join();
			threads.pop();
		}
		if (totalOutputFiles % 2 == 0) {
			totalOutputFiles /= 2;
		}
		else {
			totalOutputFiles = totalOutputFiles / 2 + 1;
		}

		underScore ^= true;
	}

}

void SortLines(vector<string> lines, unsigned int threadNumber) {
	mergeSort(lines, 0, lines.size());

	ofstream outFile(to_string(threadNumber) + ".txt");
	for (string line : lines) {
		outFile << line << endl;
	}
	outFile.close();

	totalOutputFiles ++;

}


ifstream::pos_type filesize(const char* filename)
{
    ifstream in(filename, ifstream::ate | ifstream::binary);
    return in.tellg();
}
