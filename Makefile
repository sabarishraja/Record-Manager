CFLAGS = -Wno-implicit-function-declaration
CC = gcc

ifeq ($(OS),Windows_NT)
	RM = del
	TEST1_EXECUTE_FILE= ./test_recordmgr.exe
	TEST2_EXECUTE_FILE= ./test_expr.exe
else
	RM = rm -f
	TEST1_EXECUTE_FILE= ./test_recordmgr
	TEST2_EXECUTE_FILE= ./test_expr
endif

dberror.o: dberror.c dberror.h
	echo "Compiling the dberror file"
	$(CC) $(CFLAGS) -c dberror.c

storage_mgr.o: storage_mgr.c storage_mgr.h
	echo "Compiling the storage_mgr file"
	$(CC) $(CFLAGS) -c storage_mgr.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h
	echo "Compiling the buffer_mgr file"
	$(CC) $(CFLAGS) -c buffer_mgr.c

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	echo "Compiling the buffer mgr stat file"
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	$(CC) $(CFLAGS) -c rm_serializer.c

expr.o: expr.c dberror.h record_mgr.h expr.h tables.h
	$(CC) $(CFLAGS) -c expr.c

record_mgr.o: record_mgr.c record_mgr.h buffer_mgr.h storage_mgr.h
	$(CC) $(CFLAGS) -c record_mgr.c

test_expr.o: test_expr.c dberror.h expr.h record_mgr.h tables.h test_helper.h
	$(CC) $(CFLAGS) -c test_expr.c

test_assign3_1.o: test_assign3_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	echo "Compiling the test file"
	$(CC) $(CFLAGS) -c test_assign3_1.c

test_recordmgr: test_assign3_1.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
	echo "Linking and producing the test record_mgr final file"
	$(CC) $(CFLAGS) -o test_recordmgr test_assign3_1.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o

test_expr: test_expr.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
	echo "Linking and producing the test expr final file"
	$(CC) $(CFLAGS) -o test_expr test_expr.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o

execute_test1:
	echo "Executing record manager with test 1"
	${TEST1_EXECUTE_FILE}

execute_test2:
	echo "Executing record manager with test 2"
	${TEST2_EXECUTE_FILE}

clean:
	echo "Removing all output file except source files"
	$(RM) buffer_mgr_stat.o buffer_mgr.o test_expr.o dberror.o expr.o record_mgr.o record_mgr.bin test_expr.bin test_recordmgr.bin rm_serializer.o storage_mgr.o test_assign3_1.o test_recordmgr.exe test_expr.exe test_expr test_recordmgr test_table_r test_table_t