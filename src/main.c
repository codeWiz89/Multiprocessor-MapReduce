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
	int wordCount;

	UT_hash_handle hh;
};

wordNode** allHMaps = NULL;

typedef struct file fileNode;
fileNode* filesHead = NULL;

struct file {

	char filename[50];
	fileNode* next;
};

typedef struct args argsNode;
argsNode* argsHead = NULL;

struct args {

	char filename[50];
	int index;
};

void parseArgs(int argc, char *argv[]);
void map(int numMaps, char* applicationType);
void reduce(int numReduce);
void getFilesToProcess(char* dirName);
void wordCount(int numMaps);
void* wordCount_helper(void* args);

int result[3];
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
	int ret = 0, index = 0;
	int threads = numMaps;
	pthread_t* thread = malloc(sizeof(pthread_t) * threads);
	fileNode* ptr = filesHead;

	pthread_mutex_init(&cd_lock, NULL);

	allHMaps = malloc(sizeof(wordNode) * numMaps);

	for (i = 0; i < numMaps; i++) {

		wordNode* temp = malloc(sizeof(wordNode));
		allHMaps[i] = temp;
	}


	for (i = 0; i < threads; i++) {

		argsNode* tmpArgs = malloc(sizeof(argsNode));
		strcpy(tmpArgs->filename, ptr->filename);
		tmpArgs->index = index++;

		//	ret = pthread_create(&thread[i], NULL, wordCount_helper, ptr->filename);
		//	ret = pthread_create(&thread[i], NULL, wordCount_helper, args);

		ret = pthread_create(&thread[i], NULL, wordCount_helper, tmpArgs);

		if (ret != 0) {

			printf("Create pthread error!\n");
			exit(1);
		}

		ptr = ptr->next;
	}

	for (i = 0; i < threads; i++) {

		pthread_join(thread[i], NULL);
	}

	for (i = 0; i < threads; i++) {

		printf("%d\n", result[i]);
	}
}

void* wordCount_helper(void* args) {

	argsNode* tempArgs = (argsNode*) args;
	char* fileName = tempArgs->filename;
	int index = tempArgs->index;

//	printf("filename: %s\n", fileName);
//	printf("index: %d\n", index);

	FILE* file = fopen(fileName, "r");
	char line[1000] = { 0 };

	int wordCount = 0;

	pthread_mutex_lock(&cd_lock);

	while (fgets(line, sizeof(line), file) != NULL) { //read line by line

		char * word;
		word = strtok(line, " ");

		while (word != NULL) {

			wordCount++;
		//	printf("word: %s count: %d\n", word, wordCount);
			word = strtok(NULL, " ");
		}
	}

	pthread_mutex_unlock(&cd_lock);

	result[index] = wordCount;


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
