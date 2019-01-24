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
extern unsigned long long total_time_WriteToWAL, total_count_WriteToWAL;
extern unsigned long long total_time_WriteImpl, total_count_WriteImpl;

extern unsigned long long total_time_EmitPhysicalRecord, total_count_EmitPhysicalRecord;

extern unsigned long long total_time_AppendforSST, total_count_AppendforSST;
extern unsigned long long total_time_AppendforWAL, total_count_AppendforWAL;

extern unsigned long long total_time_FlushforSST, total_count_FlushforSST;
extern unsigned long long total_time_FlushforWAL, total_count_FlushforWAL;

extern unsigned long long total_time_vblk_w_SST, total_count_vblk_w_SST;
extern unsigned long long total_time_vblk_w_WAL, total_count_vblk_w_WAL;

#endif
