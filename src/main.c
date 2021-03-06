/*
 * main.c
 *
 *  Created on: Mar 17, 2014
 *      Author: dev-1
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <ctype.h>
#include <pthread.h>
#include "uthash.h"

typedef struct commands commandList;

struct commands {

	char applicationType[25];  			// wordcount or sort
	char proceessType[25];				// processes or threads but we are only doing threads
	int numMaps;						// number of maps threads
	int numReduce;						// number of reduce threads
	char inFile[50];					// input file name
	char outFile[50];					// output file name

};
commandList* cmdList = NULL;

typedef struct word wordNode;

struct word {

	char string[50];
	int freq;

	UT_hash_handle hh;
};
wordNode* wFinalMap = NULL;

typedef struct args argsNode;

struct args {

	char fileName[50];
	wordNode* wordHMap;
	argsNode* next;

};
argsNode* argsHead = NULL;

typedef struct intNode intNode;

struct intNode {

	int num;
	int freq;

	UT_hash_handle hh;
};

typedef struct intArgs intArgsNode;

struct intArgs {

	char fileName[50];
	intNode* intHMap;
	intArgsNode* next;

};
intArgsNode* intArgsHead = NULL;

typedef struct file fileNode;
fileNode* filesHead = NULL;

struct file {

	char filename[50];
	fileNode* next;
};

void parseArgs(int argc, char *argv[]);
void map(int numMaps, char* applicationType);
void reduce(int numReduce, char* applicationType);
void getFilesToProcess(char* dirName);
void printFileList();

void wordCount_map(int numMaps);
void* wordCount_mapH(void* args);
void wordCount_reduce(int numReduce);
void* wordCount_reduceH(void* args);
int sort_by_name(wordNode*a, wordNode* b);

void intSort_map(int numMaps);
void* intSort_mapH(void* args);
void intSort_reduce(int numReduce);
void quickSort(int a[], int l, int r);
int partition(int a[], int l, int r);

pthread_mutex_t cd_lock;

void parseArgs(int argc, char *argv[]) {

	cmdList = malloc(sizeof(commandList));

	int i;

	for (i = 0; i < argc; i++) {

		if (strcmp(argv[i], "-a") == 0) {

			strcpy(cmdList->applicationType, argv[i + 1]);

		}

		else if (strcmp(argv[i], "-i") == 0) {

			strcpy(cmdList->proceessType, argv[i + 1]);

		}

		else if (strcmp(argv[i], "-m") == 0) {

			cmdList->numMaps = atoi(argv[i + 1]);

		}

		else if (strcmp(argv[i], "-r") == 0) {

			cmdList->numReduce = atoi(argv[i + 1]);
			strcpy(cmdList->inFile, argv[i + 2]);
			strcpy(cmdList->outFile, argv[i + 3]);

		}
	}
}

void map(int numMaps, char* applicationType) {

	if (strcmp(applicationType, "wordcount") == 0) {

		wordCount_map(numMaps);
	}

	else {

		intSort_map(numMaps);
	}
}

void reduce(int numReduce, char* applicationType) {

	if (strcmp(applicationType, "wordcount") == 0) {

		wordCount_reduce(numReduce);
	}

	else {

		intSort_reduce(numReduce);
	}

}

void getFilesToProcess(char* dirName) {

	DIR* dir = opendir(dirName);
	struct dirent* file;

	if (dir) {

		while ((file = readdir(dir)) != NULL) {

			if (file->d_type == DT_REG) {

				if (filesHead == NULL) {

					filesHead = malloc(sizeof(fileNode));
					memset(filesHead->filename, 0, 50);
					strcpy(filesHead->filename, dirName);
					strcat(filesHead->filename, "/");
					strcat(filesHead->filename, file->d_name);
				}

				else {

					fileNode* toAdd = malloc(sizeof(fileNode));
					fileNode* ptr = filesHead;
					fileNode* prev = NULL;

					while (ptr != NULL) {

						prev = ptr;
						ptr = ptr->next;

					}

					memset(toAdd->filename, 0, 50);
					strcpy(toAdd->filename, dirName);
					strcat(toAdd->filename, "/");
					strcat(toAdd->filename, file->d_name);
					prev->next = toAdd;
				}
			}
		}

		closedir(dir);
	}

	printFileList();

}

void wordCount_map(int numMaps) {

//	getFilesToProcess("wordcount");

	int i;
	int ret = 0;
	int threads = numMaps;
	pthread_t* thread = malloc(sizeof(pthread_t) * threads);

	fileNode* ptr = filesHead;

	for (i = 0; i < threads; i++) {

		argsNode* tmpArgs = malloc(sizeof(argsNode));
		wordNode* temp = NULL;

		memset(tmpArgs->fileName, 0, sizeof(tmpArgs->fileName));
		strcpy(tmpArgs->fileName, ptr->filename);
		tmpArgs->wordHMap = temp;

		ret = pthread_create(&thread[i], NULL, wordCount_mapH, tmpArgs);

		if (argsHead == NULL) {

			argsHead = tmpArgs;
			argsHead->next = NULL;
		}

		else {

			argsNode* ptr = argsHead;
			argsNode* prev = NULL;

			while (ptr != NULL) {

				prev = ptr;
				ptr = ptr->next;
			}

			prev->next = tmpArgs;
		}

		if (ret != 0) {

			printf("Create pthread error!\n");
			exit(1);
		}

		ptr = ptr->next;
	}

	for (i = 0; i < threads; i++) {

		pthread_join(thread[i], NULL);
	}

	argsNode* Aptr = argsHead;

	while (Aptr != NULL) {

		wordNode* ptr;

		printf("********filename: %s********\n", Aptr->fileName);

		for (ptr = Aptr->wordHMap; ptr != NULL; ptr = ptr->hh.next) {

			printf("Word: %s Count: %d\n", ptr->string, ptr->freq);

		}

		printf("********filename: %s********\n", Aptr->fileName);

		Aptr = Aptr->next;
	}
}

void* wordCount_mapH(void* args) {

	argsNode* tempArgs = (argsNode*) args;

	char* fileName = tempArgs->fileName;
	wordNode* wordHM = tempArgs->wordHMap;

	FILE* file = fopen(fileName, "r");
	char line[1000] = { 0 };
	char* word;
	wordNode* toFind;

	while (!feof(file)) {

		fgets(line, sizeof(line), file);

		int len = strlen(line);

		if (len > 0 && line[len - 1] == '\n') {

			line[len - 1] = '\0';
		}

		word = strtok(line, " ");

		while (word != NULL) {

			HASH_FIND_STR(wordHM, word, toFind);

			if (toFind) {

				toFind->freq += 1;

			}

			else {

				wordNode* toAdd = malloc(sizeof(wordNode));
				strcpy(toAdd->string, word);
				toAdd->freq = 1;

				HASH_ADD_STR(wordHM, string, toAdd);

				/*	wordNode* ptr;

				 for (ptr = wordHM; ptr != NULL; ptr = ptr->hh.next) {

				 printf("Word: %s Count: %d\n", ptr->string, ptr->freq);

				 } */
			}

			word = strtok(NULL, " ");

		}

		memset(line, 0, 1000);
	}

	tempArgs->wordHMap = wordHM;

	return 0;
}

void wordCount_reduce(int numReduce) {

	argsNode* Aptr = argsHead;

	int i, ret;
	int threads = numReduce;
	pthread_t* thread = malloc(sizeof(pthread_t) * threads);

	for (i = 0; i < threads; i++) {

		ret = pthread_create(&thread[i], NULL, wordCount_reduceH, Aptr);

		if (ret != 0) {

			printf("Create pthread error!\n");
			exit(1);
		}

		Aptr = Aptr->next;
	}

	for (i = 0; i < threads; i++) {

		pthread_join(thread[i], NULL);
	}

	HASH_SRT(hh, wFinalMap, sort_by_name);

	/*	printf("\n");
	 printf("\n");
	 printf("******printing final map******\n");

	 wordNode* tmpS;

	 for (tmpS = wFinalMap; tmpS != NULL; tmpS = tmpS->hh.next) {

	 printf("%s %d\n", tmpS->string, tmpS->freq);

	 }

	 printf("******done printing final map******\n"); */

	FILE* outputFile = fopen(cmdList->outFile, "w+");

	wordNode* tmpS;

	for (tmpS = wFinalMap; tmpS != NULL; tmpS = tmpS->hh.next) {

		fprintf(outputFile, "%s ", tmpS->string);
		fprintf(outputFile, "%d ", tmpS->freq);

		fprintf(outputFile, "\n");

	}

}

void* wordCount_reduceH(void* args) {

	pthread_mutex_lock(&cd_lock);

	argsNode* tempArgs = (argsNode*) args;
	wordNode* wordHM = tempArgs->wordHMap;

	wordNode* tmpPtr;
	wordNode* toFind = NULL;

	for (tmpPtr = wordHM; tmpPtr != NULL; tmpPtr = tmpPtr->hh.next) {

		char* word = tmpPtr->string;

		HASH_FIND_STR(wFinalMap, word, toFind);

		if (toFind) {

			toFind->freq += 1;

		}

		else {

			wordNode* toAdd = malloc(sizeof(wordNode));
			strcpy(toAdd->string, tmpPtr->string);
			toAdd->freq = tmpPtr->freq;

			HASH_ADD_STR(wFinalMap, string, toAdd);
		}
	}

	pthread_mutex_unlock(&cd_lock);

	return 0;
}

int sort_by_name(wordNode*a, wordNode* b) {

	return strcmp(a->string, b->string);
}

void intSort_map(int numMaps) {

	int i;
	int ret = 0;
	int threads = numMaps;
	pthread_t* thread = malloc(sizeof(pthread_t) * threads);

	fileNode* ptr = filesHead;

	for (i = 0; i < threads; i++) {

		intArgsNode* tmpArgs = malloc(sizeof(intArgsNode));
		intNode* temp = NULL;

		memset(tmpArgs->fileName, 0, sizeof(tmpArgs->fileName));
		strcpy(tmpArgs->fileName, ptr->filename);
		tmpArgs->intHMap = temp;

		if (intArgsHead == NULL) {

			intArgsHead = tmpArgs;
			intArgsHead->next = NULL;
		}

		else {

			intArgsNode* ptr = intArgsHead;
			intArgsNode* prev = NULL;

			while (ptr != NULL) {

				prev = ptr;
				ptr = ptr->next;
			}

			prev->next = tmpArgs;
		}

		ret = pthread_create(&thread[i], NULL, intSort_mapH, tmpArgs);

		if (ret != 0) {

			printf("Create pthread error!\n");
			exit(1);
		}

		ptr = ptr->next;
	}

	for (i = 0; i < threads; i++) {

		pthread_join(thread[i], NULL);
	}

	intArgsNode* Iptr = intArgsHead;

	while (Iptr != NULL) {

		intNode* ptr;

		printf("********filename: %s********\n", Iptr->fileName);

		for (ptr = Iptr->intHMap; ptr != NULL; ptr = ptr->hh.next) {

			printf("int: %d\n", ptr->num);

		}

		printf("********filename: %s********\n", Iptr->fileName);

		Iptr = Iptr->next;
	}

}

void* intSort_mapH(void* args) {

	intArgsNode* tempArgs = (intArgsNode*) args;

	char* fileName = tempArgs->fileName;
	intNode* intHM = tempArgs->intHMap;

	FILE* file = fopen(fileName, "r");
	char line[1000] = { 0 };
	char* word;
	intNode* toFind;

	while (!feof(file)) {

		fgets(line, sizeof(line), file);

		int len = strlen(line);

		if (len > 0 && line[len - 1] == '\n') {

			line[len - 1] = '\0';
		}

		word = strtok(line, " ");

		while (word != NULL) {

			int tempNum = atoi(word);

			HASH_FIND_INT(intHM, &tempNum, toFind);

			if (!toFind) {

				intNode* toAdd = malloc(sizeof(intNode));
				toAdd->num = tempNum;

				HASH_ADD_INT(intHM, num, toAdd);
			}

			word = strtok(NULL, " ");
		}

		memset(line, 0, 1000);
	}

	tempArgs->intHMap = intHM;

	return 0;

}

void intSort_reduce(int numReduce) {

	intArgsNode* Iptr = intArgsHead;

	int i;
	int threads = numReduce;

	int arrayLength = 0;

	for (i = 0; i < threads; i++) {

		arrayLength += HASH_COUNT(Iptr->intHMap);
		Iptr = Iptr->next;
	}

	int* allNums = malloc(sizeof(int) * arrayLength);
	Iptr = intArgsHead;

	intNode* ptr = NULL;
	int insertCount = 0;

	while (Iptr != NULL) {

		for (ptr = Iptr->intHMap; ptr != NULL; ptr = ptr->hh.next) {

			allNums[insertCount] = ptr->num;
			insertCount++;

		}

		Iptr = Iptr->next;
	}

	printf("****Unsorted****\n");

	for (i = 0; i < arrayLength; i++) {

		printf("%d ", allNums[i]);

	}

	printf("\n");
	printf("****Unsorted****\n");

	printf("****Sorted****\n");

	quickSort(allNums, 0, arrayLength);

	for (i = 0; i < arrayLength; i++) {

		printf("%d ", allNums[i]);

	}

	printf("\n");
	printf("****Sorted****\n");

	FILE* outputFile = fopen(cmdList->outFile, "w+");

	for (i = 0; i < arrayLength; i++) {

		fprintf(outputFile, "%d", allNums[i]);
		fprintf(outputFile, "\n");

	}

	free(allNums);

}

void quickSort(int a[], int l, int r) {

	int j;

	if (l < r) {
		// divide and conquer
		j = partition(a, l, r);
		quickSort(a, l, j - 1);
		quickSort(a, j + 1, r);
	}
}

int partition(int a[], int l, int r) {

	int pivot, i, j, t;
	pivot = a[l];
	i = l;
	j = r + 1;

	while (1) {
		do
			++i;
		while (a[i] <= pivot && i <= r);
		do
			--j;
		while (a[j] > pivot);
		if (i >= j)
			break;
		t = a[i];
		a[i] = a[j];
		a[j] = t;
	}

	t = a[l];
	a[l] = a[j];
	a[j] = t;

	return j;
}

void printCommandList() {

	printf("Application type: %s\n", cmdList->applicationType);
	printf("Process type: %s\n", cmdList->proceessType);
	printf("# of maps: %d\n", cmdList->numMaps);
	printf("# of reduce: %d\n", cmdList->numReduce);
	printf("Input File: %s\n", cmdList->inFile);
	printf("Output File: %s\n", cmdList->outFile);
}

void printFileList() {

	fileNode* ptr = filesHead;

	while (ptr != NULL) {

		printf("%s\n", ptr->filename);
		ptr = ptr->next;
	}
}

void clearAll() {

	if (wFinalMap != NULL) {

		HASH_CLEAR(hh, wFinalMap);
	}

	if (intArgsHead != NULL) {

		intArgsNode* ptr = intArgsHead;

		while (ptr != NULL) {

			intNode* temp = ptr->intHMap;
			HASH_CLEAR(hh, temp);
		}
	}

	if (argsHead != NULL) {

		argsNode* ptr = argsHead;

		while (ptr != NULL) {

			wordNode* temp = ptr->wordHMap;
			HASH_CLEAR(hh, temp);
		}
	}

}

int main(int argc, char *argv[]) {

	if (argc > 11 || argc < 11) {

		printf("Invalid number of arguments...\n");
		return 0;

	}

	parseArgs(argc, argv);
//	printCommandList();

	getFilesToProcess(cmdList->applicationType);
	int fileCount = 0;
	fileNode* ptr = filesHead;

	while (ptr != NULL) {

		fileCount++;
		ptr = ptr->next;
	}

	if (strcmp(cmdList->applicationType, "wordcount") == 0) {

		//	map(cmdList->numMaps, "wordcount");
		//	reduce(cmdList->numReduce, "wordcount");

		map(fileCount, "wordcount");
		reduce(fileCount, "wordcount");

	}

	else {

		map(fileCount, "sort");
		reduce(fileCount, "sort");

	}

	clearAll();

	return 0;
}
