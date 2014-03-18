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
wordNode* word = NULL;

struct word {

	char string[50];
	int wordCount;

	UT_hash_handle hh;
};

typedef struct file fileNode;
fileNode* filesHead = NULL;

struct file {

	char filename[50];
	fileNode* next;
};

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

void map(int numMaps) {

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
					strcpy(filesHead->filename, file->d_name);
				}

				else {

					fileNode* toAdd = malloc(sizeof(fileNode));
					fileNode* ptr = filesHead;
					fileNode* prev = NULL;

					while (ptr != NULL) {

						prev = ptr;
						ptr = ptr->next;

					}

					strcpy(toAdd->filename, file->d_name);
					prev->next = toAdd;
				}
			}
		}

		closedir(dir);
	}

	printFileList();

}

void wordCount() {

	getFilesToProcess("wordcount");

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

	/*	int i = 0;

	 for (i = 0; i < argc; i++) {

	 printf("%s\n", argv[i]);
	 }

	 printf("%d\n", argc);

	 */

	if (argc > 11 || argc < 11) {

		printf("Invalid number of arguments...\n");
		return 0;

	}

	parseArgs(argc, argv);
	printCommandList();

	if (strcmp(cmdList->applicationType, "wordcount") == 0) {

		wordCount();
	}

	return 0;
}
