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
void reduce(int numReduce);
void getFilesToProcess(char* dirName);
void wordCount(int numMaps);
void* wordCount_helper(void* args);

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

		wordCount(numMaps);
	}
}

void reduce(int numReduce) {

}

void printFileList() {

	fileNode* ptr = filesHead;

	while (ptr != NULL) {

		printf("%s\n", ptr->filename);
		ptr = ptr->next;
	}
}

void getFilesToProcess(char* dirName) {

	DIR* dir = opendir(dirName);
	struct dirent* file;

	if (dir) {

		while ((file = readdir(dir)) != NULL) {

			//	printf("%s\n", file->d_name);

			if (file->d_type == DT_REG) {

				if (filesHead == NULL) {

					filesHead = malloc(sizeof(fileNode));
					strcpy(filesHead->filename, "wordcount/");
					strcat(filesHead->filename, file->d_name);
					//	strcpy(filesHead->filename, file->d_name);
				}

				else {

					fileNode* toAdd = malloc(sizeof(fileNode));
					fileNode* ptr = filesHead;
					fileNode* prev = NULL;

					while (ptr != NULL) {

						prev = ptr;
						ptr = ptr->next;

					}

					//strcpy(toAdd->filename, file->d_name);
					strcpy(toAdd->filename, "wordcount/");
					strcat(toAdd->filename, file->d_name);
					prev->next = toAdd;
				}
			}
		}

		closedir(dir);
	}

	printFileList();

}

void wordCount(int numMaps) {

	getFilesToProcess("wordcount");

	int i;
	int ret = 0;
	int threads = numMaps;
	pthread_t* thread = malloc(sizeof(pthread_t) * threads);

	fileNode* ptr = filesHead;

	argsNode* head = NULL;

	for (i = 0; i < threads; i++) {

		argsNode* tmpArgs = malloc(sizeof(argsNode));
		wordNode* temp = NULL;

		strcpy(tmpArgs->fileName, ptr->filename);
		tmpArgs->wordHMap = temp;

		ret = pthread_create(&thread[i], NULL, wordCount_helper, tmpArgs);

		if (head == NULL) {

			head = tmpArgs;
			head->next = NULL;
		}

		else {

			argsNode* ptr = head;
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

	argsNode* Aptr = head;

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

void* wordCount_helper(void* args) {

	argsNode* tempArgs = (argsNode*) args;

	char* fileName = tempArgs->fileName;
	wordNode* wordHM = tempArgs->wordHMap;

//	printf("********filename: %s\n********", fileName);

	FILE* file = fopen(fileName, "r");
	char line[1000] = { 0 };
	char* word;
	wordNode* toFind;

	while (!feof(file)) {

		fgets(line, sizeof(line), file);
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
	}

	tempArgs->wordHMap = wordHM;

	return 0;
}

void printCommandList() {

	printf("Application type: %s\n", cmdList->applicationType);
	printf("Process type: %s\n", cmdList->proceessType);
	printf("# of maps: %d\n", cmdList->numMaps);
	printf("# of reduce: %d\n", cmdList->numReduce);
	printf("Input File: %s\n", cmdList->inFile);
	printf("Output File: %s\n", cmdList->outFile);
}

int main(int argc, char *argv[]) {

	if (argc > 11 || argc < 11) {

		printf("Invalid number of arguments...\n");
		return 0;

	}

	parseArgs(argc, argv);
	printCommandList();

	if (strcmp(cmdList->applicationType, "wordcount") == 0) {

		map(cmdList->numMaps, "wordcount");

	}

	return 0;
}
