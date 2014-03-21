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
commandList* cmdList = NULL;

struct commands {

	char applicationType[25];  			// wordcount or sort
	char proceessType[25];				// processes or threads but we are only doing threads
	int numMaps;						// number of maps threads
	int numReduce;						// number of reduce threads
	char inFile[50];					// input file name
	char outFile[50];					// output file name

};

typedef struct word wordNode;

struct word {

	char string[50];
	int freq;

	UT_hash_handle hh;
};
wordNode* finalMap = NULL;

typedef struct alphaChar alphaCharNode;

struct alphaChar {

	char c;
	int index;

	UT_hash_handle hh;
};

typedef struct file fileNode;
fileNode* filesHead = NULL;

struct file {

	char filename[50];
	fileNode* next;
};

typedef struct args argsNode;
argsNode* argsHead = NULL;

struct args {

	char fileName[50];
	wordNode* wordHMap;
	argsNode* next;

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

	HASH_SRT(hh, finalMap, sort_by_name);

	printf("\n");
	printf("\n");
	printf("******printing final map******\n");

	wordNode* tmpS;

	for (tmpS = finalMap; tmpS != NULL; tmpS = tmpS->hh.next) {

		printf("%s %d\n", tmpS->string, tmpS->freq);

	}

	printf("******done printing final map******\n");
}

void* wordCount_reduceH(void* args) {

	pthread_mutex_lock(&cd_lock);

	argsNode* tempArgs = (argsNode*) args;
	wordNode* wordHM = tempArgs->wordHMap;

	wordNode* tmpPtr;
	wordNode* toFind = NULL;

	for (tmpPtr = wordHM; tmpPtr != NULL; tmpPtr = tmpPtr->hh.next) {

		char* word = tmpPtr->string;

		HASH_FIND_STR(finalMap, word, toFind);

		if (toFind) {

			toFind->freq += 1;

		}

		else {

			wordNode* toAdd = malloc(sizeof(wordNode));
			strcpy(toAdd->string, tmpPtr->string);
			toAdd->freq = tmpPtr->freq;

			HASH_ADD_STR(finalMap, string, toAdd);
		}
	}

	pthread_mutex_unlock(&cd_lock);

	return 0;
}

int sort_by_name(wordNode*a, wordNode* b) {

	return strcmp(a->string, b->string);
}

void intSort_map(int numMaps) {

	getFilesToProcess("sort");

	int i;
	int ret = 0;
	int threads = numMaps;
	pthread_t* thread = malloc(sizeof(pthread_t) * threads);

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

		map(cmdList->numMaps, "sort");

	}

	return 0;
}
