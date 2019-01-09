#ifndef _PROF_H
#define _PROF_H

#include <stdio.h>
#include <time.h>
#include <stdlib.h>

//function level profiling
#define BILLION     (1000000000ULL)
#define calclock(timevalue, total_time, total_count) do { \
		unsigned long long timedelay, temp, temp_n; \
		struct timespec *myclock = (struct timespec*)timevalue; \
		if(myclock[1].tv_nsec >= myclock[0].tv_nsec){ \
				temp = myclock[1].tv_sec - myclock[0].tv_sec; \
				temp_n = myclock[1].tv_nsec - myclock[0].tv_nsec; \
				timedelay = BILLION * temp + temp_n; \
		} else { \
				temp = myclock[1].tv_sec - myclock[0].tv_sec - 1; \
				temp_n = BILLION + myclock[1].tv_nsec - myclock[0].tv_nsec; \
				timedelay = BILLION * temp + temp_n; \
		} \
		__sync_fetch_and_add(total_time, timedelay); \
		__sync_fetch_and_add(total_count, 1); \
} while(0)


//Global function time/count variables
extern unsigned long long total_time_tb, total_count_tb;
extern unsigned long long total_time_WriteToWAL, total_count_WriteToWAL;
extern unsigned long long total_time_WriteImpl, total_count_WriteImpl;

#endif
