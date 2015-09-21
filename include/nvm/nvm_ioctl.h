#ifndef _NVM_IOCTL_H_
#define _NVM_IOCTL_H_

//TODO: Javier: Assign new magic numbers to ioctls

#define NVMBLOCKPUT             21525
#define NVMBLOCKRRGET           21526
#define NVMLUNSNRGET            21527
#define NVMBLOCKSNRGET          21528
#define NVMBLOCKERASE           21529
#define NVMPAGESNRGET           21530
#define NVMBLOCKGETBYADDR       21531
#define NVMBLOCKGETBYID         21532
#define NVMPAGESIZEGET          21533
#define NVMCHANNELSNRGET        21534

#define NVM_DEVSECTSIZE_GET     21535
#define NVM_DEVMAXSECT_GET      21536

#define NVM_GET_BLOCK           21526
#define NVM_PUT_BLOCK           21525
#define NVM_GET_BLOCK_META      21537

#endif //_NVM_IOCTL_H_
