#ifndef _NVM_MEM_H_
#define _NVM_MEM_H_

#define ALLOC_CLASS(x, y) \
x = new y;\
if(!(x))\
{\
    printf("out of memory\n");\
    exit(EXIT_FAILURE);\
}

#define ALLOC_STRUCT(x, y, z) \
(x) = (z *)malloc((y) * sizeof(z));\
if(!(x))\
{\
    printf("out of memory\n");\
    exit(EXIT_FAILURE);\
}

#define SAFE_ALLOC(x, y) \
x = new y;\
if(!(x))\
{\
    printf("out of memory\n");\
    exit(EXIT_FAILURE);\
}

#define SAFE_MALLOC(x, y, z) ALLOC_STRUCT(x, y, z)

#endif /* _NVM_MEM_H_ */
