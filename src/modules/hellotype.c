/* This file implements a new module native data type called "HELLOTYPE".
 * The data structure implemented is a very simple ordered linked list of
 * 64 bit integers, in order to have something that is real world enough, but
 * at the same time, extremely simple to understand, to show how the API
 * works, how a new data type is created, and how to write basic methods
 * for RDB loading, saving and AOF rewriting.
 * 
 * 这个文件实现了一个名为“HELLOTYPE”的新的模块原生数据类型。
 * 实现的数据结构是一个非常简单的命令链表的64位整数,
 * 为了在保证数据的真实性的同时，又要做到容易理解。
 * 这个文件说明了API是如何工作的,一个新的数据类型是如何创建的,
 * 以及如何编写基本方法来进行RDB的加载、保存和AOF重写。
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2016, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "../redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stdint.h>

static RedisModuleType *HelloType;

/* ========================== Internal data structure  =======================
 * This is just a linked list of 64 bit integers where elements are inserted
 * in-place, so it's ordered. There is no pop/push operation but just insert
 * because it is enough to show the implementation of new data types without
 * making things complex.
 * 这只是一个由64位整数组成的链表，其中的元素是在适当位置插入的，因此它是有序的。
 * 这里没有pop/push操作，只有insert操作，因为它足以展示新数据类型的实现，而又不会使事情变得复杂。
 *  */

struct HelloTypeNode {
    int64_t value;
    struct HelloTypeNode *next;
};

struct HelloTypeObject {
    struct HelloTypeNode *head;
    size_t len; /* Number of elements added. */
};

// 创建一个空链表
struct HelloTypeObject *createHelloTypeObject(void) {
    struct HelloTypeObject *o;
    // RedisModule_Alloc 申请内存
    o = RedisModule_Alloc(sizeof(*o));
    o->head = NULL;
    o->len = 0;
    return o;
}

// 向链表中插入 ele
void HelloTypeInsert(struct HelloTypeObject *o, int64_t ele) {
    struct HelloTypeNode *next = o->head, *newnode, *prev = NULL;

    while(next && next->value < ele) {
        prev = next;
        next = next->next;
    }
    newnode = RedisModule_Alloc(sizeof(*newnode));
    newnode->value = ele;
    newnode->next = next;
    if (prev) {
        prev->next = newnode;
    } else {
        o->head = newnode;
    }
    o->len++;
}

void HelloTypeReleaseObject(struct HelloTypeObject *o) {
    struct HelloTypeNode *cur, *next;
    cur = o->head;
    while(cur) {
        next = cur->next;
        RedisModule_Free(cur);
        cur = next;
    }
    RedisModule_Free(o);
}

/* ========================= "hellotype" type commands ======================= */

/* HELLOTYPE.INSERT key value */
int HelloTypeInsert_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != HelloType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    long long value;
    if ((RedisModule_StringToLongLong(argv[2],&value) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    /* Create an empty value object if the key is currently empty. */
    struct HelloTypeObject *hto;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        hto = createHelloTypeObject();
        RedisModule_ModuleTypeSetValue(key,HelloType,hto);
    } else {
        hto = RedisModule_ModuleTypeGetValue(key);
    }

    /* Insert the new element. */
    HelloTypeInsert(hto,value);

    RedisModule_ReplyWithLongLong(ctx,hto->len);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* HELLOTYPE.RANGE key first count */
int HelloTypeRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 4) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != HelloType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    long long first, count;
    if (RedisModule_StringToLongLong(argv[2],&first) != REDISMODULE_OK ||
        RedisModule_StringToLongLong(argv[3],&count) != REDISMODULE_OK ||
        first < 0 || count < 0)
    {
        return RedisModule_ReplyWithError(ctx,
            "ERR invalid first or count parameters");
    }

    struct HelloTypeObject *hto = RedisModule_ModuleTypeGetValue(key);
    struct HelloTypeNode *node = hto ? hto->head : NULL;
    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    long long arraylen = 0;
    while(node && count--) {
        RedisModule_ReplyWithLongLong(ctx,node->value);
        arraylen++;
        node = node->next;
    }
    RedisModule_ReplySetArrayLength(ctx,arraylen);
    return REDISMODULE_OK;
}

/* HELLOTYPE.LEN key */
int HelloTypeLen_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != HelloType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    struct HelloTypeObject *hto = RedisModule_ModuleTypeGetValue(key);
    RedisModule_ReplyWithLongLong(ctx,hto ? hto->len : 0);
    return REDISMODULE_OK;
}


/* ========================== "hellotype" type methods ======================= */

void *HelloTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        /* RedisModule_Log("warning","Can't load data with version %d", encver);*/
        return NULL;
    }
    uint64_t elements = RedisModule_LoadUnsigned(rdb);
    struct HelloTypeObject *hto = createHelloTypeObject();
    while(elements--) {
        int64_t ele = RedisModule_LoadSigned(rdb);
        HelloTypeInsert(hto,ele);
    }
    return hto;
}

void HelloTypeRdbSave(RedisModuleIO *rdb, void *value) {
    struct HelloTypeObject *hto = value;
    struct HelloTypeNode *node = hto->head;
    RedisModule_SaveUnsigned(rdb,hto->len);
    while(node) {
        RedisModule_SaveSigned(rdb,node->value);
        node = node->next;
    }
}

void HelloTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    struct HelloTypeObject *hto = value;
    struct HelloTypeNode *node = hto->head;
    while(node) {
        RedisModule_EmitAOF(aof,"HELLOTYPE.INSERT","sl",key,node->value);
        node = node->next;
    }
}

/* The goal of this function is to return the amount of memory used by
 * the HelloType value. */
size_t HelloTypeMemUsage(const void *value) {
    const struct HelloTypeObject *hto = value;
    struct HelloTypeNode *node = hto->head;
    return sizeof(*hto) + sizeof(*node)*hto->len;
}

void HelloTypeFree(void *value) {
    HelloTypeReleaseObject(value);
}

void HelloTypeDigest(RedisModuleDigest *md, void *value) {
    struct HelloTypeObject *hto = value;
    struct HelloTypeNode *node = hto->head;
    while(node) {
        RedisModule_DigestAddLongLong(md,node->value);
        node = node->next;
    }
    RedisModule_DigestEndSequence(md);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server.
 * 模块入口，每个模块必须实现该函数。
 * 调用RedisModule_Init注册模块，调用RedisModule_CreateCommand注册命令
  */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    // 初始化 hellotype 模块，hellotype 是模块名称 和文件名没关系，想定义什么定义什么。但最好和模块同名。
    if (RedisModule_Init(ctx,"hellotype",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = HelloTypeRdbLoad,
        .rdb_save = HelloTypeRdbSave,
        .aof_rewrite = HelloTypeAofRewrite,
        .mem_usage = HelloTypeMemUsage,
        .free = HelloTypeFree,
        .digest = HelloTypeDigest
    };

    // 注册 hellotype 数据持久化相关的处理逻辑
    HelloType = RedisModule_CreateDataType(ctx,"hellotype",0,&tm);
    if (HelloType == NULL) return REDISMODULE_ERR;

    // 注册 hellotype.insert 命令对应的处理逻辑
    if (RedisModule_CreateCommand(ctx,"hellotype.insert",
        HelloTypeInsert_RedisCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // 注册 hellotype.range 命令对应的处理逻辑
    if (RedisModule_CreateCommand(ctx,"hellotype.range",
        HelloTypeRange_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // 注册 hellotype.len 命令对应的处理逻辑
    if (RedisModule_CreateCommand(ctx,"hellotype.len",
        HelloTypeLen_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
