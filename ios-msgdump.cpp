/**
 * ios-msgdump; extract iMessages from iOS backups.
 *
 * Please see the file LICENSE.txt in the source's root directory.
 *
 *  This file written by Ryan C. Gordon.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <stdarg.h>

#include "sqlite3/sqlite3.h"

typedef int8_t sint8;
typedef uint8_t uint8;
typedef int16_t sint16;
typedef uint16_t uint16;
typedef int32_t sint32;
typedef uint32_t uint32;
typedef int64_t sint64;
typedef uint64_t uint64;

//#include <map>

#if defined(__clang__)
#if __has_feature(attribute_analyzer_noreturn)
#define NORETURN __attribute__((analyzer_noreturn))
#endif
#endif

#ifndef NORETURN
#define NORETURN
#endif

#if defined(__GNUC__) || defined(__clang__)
#define ISPRINTF(x,y) __attribute__((format (printf, x, y)))
#endif

#ifndef ISPRINTF
#define ISPRINTF
#endif


// SHA-1 code originally from ftp://ftp.funet.fi/pub/crypt/hash/sha/sha1.c
//  License: public domain.
//  I cleaned it up a little for my specific purposes. --ryan.

typedef struct
{
    uint32 state[5];
    uint32 count[2];
    uint8 buffer[64];
} MojoSha1;

/*
SHA-1 in C
By Steve Reid <steve@edmweb.com>
100% Public Domain

Test Vectors (from FIPS PUB 180-1)
"abc"
  A9993E36 4706816A BA3E2571 7850C26C 9CD0D89D
"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
  84983E44 1C3BD26E BAAE4AA1 F95129E5 E54670F1
A million repetitions of "a"
  34AA973C D4C4DAA4 F61EEB2B DBAD2731 6534016F
*/

#define rol(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))

/* blk0() and blk() perform the initial expand. */
/* I got the idea of expanding during the round function from SSLeay */
#if 1 //PLATFORM_LITTLEENDIAN
#define blk0(i) (block->l[i] = (rol(block->l[i],24)&0xFF00FF00) \
    |(rol(block->l[i],8)&0x00FF00FF))
#else
#define blk0(i) block->l[i]
#endif
#define blk(i) (block->l[i&15] = rol(block->l[(i+13)&15]^block->l[(i+8)&15] \
    ^block->l[(i+2)&15]^block->l[i&15],1))

/* (R0+R1), R2, R3, R4 are the different operations used in SHA1 */
#define R0(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk0(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define R1(v,w,x,y,z,i) z+=((w&(x^y))^y)+blk(i)+0x5A827999+rol(v,5);w=rol(w,30);
#define R2(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0x6ED9EBA1+rol(v,5);w=rol(w,30);
#define R3(v,w,x,y,z,i) z+=(((w|x)&y)|(w&x))+blk(i)+0x8F1BBCDC+rol(v,5);w=rol(w,30);
#define R4(v,w,x,y,z,i) z+=(w^x^y)+blk(i)+0xCA62C1D6+rol(v,5);w=rol(w,30);


/* Hash a single 512-bit block. This is the core of the algorithm. */

static void MojoSha1_transform(uint32 state[5], const uint8 buffer[64])
{
    uint32 a, b, c, d, e;
    typedef union {
        uint8 c[64];
        uint32 l[16];
    } CHAR64LONG16;
    CHAR64LONG16* block;
    static uint8 workspace[64];
    block = (CHAR64LONG16*)workspace;
    memcpy(block, buffer, 64);
    /* Copy context->state[] to working vars */
    a = state[0];
    b = state[1];
    c = state[2];
    d = state[3];
    e = state[4];
    /* 4 rounds of 20 operations each. Loop unrolled. */
    R0(a,b,c,d,e, 0); R0(e,a,b,c,d, 1); R0(d,e,a,b,c, 2); R0(c,d,e,a,b, 3);
    R0(b,c,d,e,a, 4); R0(a,b,c,d,e, 5); R0(e,a,b,c,d, 6); R0(d,e,a,b,c, 7);
    R0(c,d,e,a,b, 8); R0(b,c,d,e,a, 9); R0(a,b,c,d,e,10); R0(e,a,b,c,d,11);
    R0(d,e,a,b,c,12); R0(c,d,e,a,b,13); R0(b,c,d,e,a,14); R0(a,b,c,d,e,15);
    R1(e,a,b,c,d,16); R1(d,e,a,b,c,17); R1(c,d,e,a,b,18); R1(b,c,d,e,a,19);
    R2(a,b,c,d,e,20); R2(e,a,b,c,d,21); R2(d,e,a,b,c,22); R2(c,d,e,a,b,23);
    R2(b,c,d,e,a,24); R2(a,b,c,d,e,25); R2(e,a,b,c,d,26); R2(d,e,a,b,c,27);
    R2(c,d,e,a,b,28); R2(b,c,d,e,a,29); R2(a,b,c,d,e,30); R2(e,a,b,c,d,31);
    R2(d,e,a,b,c,32); R2(c,d,e,a,b,33); R2(b,c,d,e,a,34); R2(a,b,c,d,e,35);
    R2(e,a,b,c,d,36); R2(d,e,a,b,c,37); R2(c,d,e,a,b,38); R2(b,c,d,e,a,39);
    R3(a,b,c,d,e,40); R3(e,a,b,c,d,41); R3(d,e,a,b,c,42); R3(c,d,e,a,b,43);
    R3(b,c,d,e,a,44); R3(a,b,c,d,e,45); R3(e,a,b,c,d,46); R3(d,e,a,b,c,47);
    R3(c,d,e,a,b,48); R3(b,c,d,e,a,49); R3(a,b,c,d,e,50); R3(e,a,b,c,d,51);
    R3(d,e,a,b,c,52); R3(c,d,e,a,b,53); R3(b,c,d,e,a,54); R3(a,b,c,d,e,55);
    R3(e,a,b,c,d,56); R3(d,e,a,b,c,57); R3(c,d,e,a,b,58); R3(b,c,d,e,a,59);
    R4(a,b,c,d,e,60); R4(e,a,b,c,d,61); R4(d,e,a,b,c,62); R4(c,d,e,a,b,63);
    R4(b,c,d,e,a,64); R4(a,b,c,d,e,65); R4(e,a,b,c,d,66); R4(d,e,a,b,c,67);
    R4(c,d,e,a,b,68); R4(b,c,d,e,a,69); R4(a,b,c,d,e,70); R4(e,a,b,c,d,71);
    R4(d,e,a,b,c,72); R4(c,d,e,a,b,73); R4(b,c,d,e,a,74); R4(a,b,c,d,e,75);
    R4(e,a,b,c,d,76); R4(d,e,a,b,c,77); R4(c,d,e,a,b,78); R4(b,c,d,e,a,79);
    /* Add the working vars back into context.state[] */
    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
    state[4] += e;
}


/* MojoSha1_init - Initialize new context */

void MojoSha1_init(MojoSha1 *context)
{
    /* SHA1 initialization constants */
    context->state[0] = 0x67452301;
    context->state[1] = 0xEFCDAB89;
    context->state[2] = 0x98BADCFE;
    context->state[3] = 0x10325476;
    context->state[4] = 0xC3D2E1F0;
    context->count[0] = context->count[1] = 0;
}


/* Run your data through this. */

void MojoSha1_append(MojoSha1 *context, const void *_data, uint32 len)
{
    const uint8 *data = (const uint8 *) _data;
    uint32 i, j;

    j = (context->count[0] >> 3) & 63;
    if ((context->count[0] += len << 3) < (len << 3)) context->count[1]++;
    context->count[1] += (len >> 29);
    if ((j + len) > 63) {
        memcpy(&context->buffer[j], data, (i = 64-j));
        MojoSha1_transform(context->state, context->buffer);
        for ( ; i + 63 < len; i += 64) {
            MojoSha1_transform(context->state, &data[i]);
        }
        j = 0;
    }
    else i = 0;
    memcpy(&context->buffer[j], &data[i], len - i);
}


/* Add padding and return the message digest. */

void MojoSha1_finish(MojoSha1 *context, uint8 digest[20])
{
    uint32 i;
    uint8 finalcount[8];

    for (i = 0; i < 8; i++) {
        finalcount[i] = (uint8)((context->count[(i >= 4 ? 0 : 1)]
         >> ((3-(i & 3)) * 8) ) & 255);  /* Endian independent */
    }
    MojoSha1_append(context, (uint8 *)"\200", 1);
    while ((context->count[0] & 504) != 448) {
        MojoSha1_append(context, (uint8 *)"\0", 1);
    }
    MojoSha1_append(context, finalcount, 8);  /* Should cause a MojoSha1_transform() */
    for (i = 0; i < 20; i++) {
        digest[i] = (uint8)
         ((context->state[i>>2] >> ((3-(i & 3)) * 8) ) & 255);
    }
    /* Wipe variables */
    memset(context->buffer, 0, 64);
    memset(context->state, 0, 20);
    memset(context->count, 0, 8);
    memset(&finalcount, 0, 8);
    MojoSha1_transform(context->state, context->buffer);
}

#if 0
static void Sha1(const void *buf, const size_t buflen, uint8 *digest)
{
    MojoSha1 ctx;
    MojoSha1_init(&ctx);
    MojoSha1_append(&ctx, buf, buflen);
    MojoSha1_finish(&ctx, digest);
} // Sha1
#endif

static void archiveFname(const char *domain, const char *name, char *fname)
{
    uint8 digest[20];
    MojoSha1 ctx;

    MojoSha1_init(&ctx);
    MojoSha1_append(&ctx, domain, strlen(domain));
    MojoSha1_append(&ctx, "-", 1);
    MojoSha1_append(&ctx, name, strlen(name));
    MojoSha1_finish(&ctx, digest);

    snprintf(fname, 41, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
        (int) digest[0],  (int) digest[1],  (int) digest[2],  (int) digest[3],
        (int) digest[4],  (int) digest[5],  (int) digest[6],  (int) digest[7],
        (int) digest[8],  (int) digest[9],  (int) digest[10], (int) digest[11],
        (int) digest[12], (int) digest[13], (int) digest[14], (int) digest[15],
        (int) digest[16], (int) digest[17], (int) digest[18], (int) digest[19]);
} // archiveFname


#if 0
static FILE *fopenFromArchive(const char *domain, const char *name)
{
    char fname[41];
    archiveFname(domain, name, fname);
    return fopen(fname, "rb");
} // fopenFromArchive
#endif


static sqlite3 *sqlite3FromArchive(const char *domain, const char *name)
{
    char fname[41];
    archiveFname(domain, name, fname);

printf("db fname == '%s'\n", fname);
    sqlite3 *db = NULL;
    if (sqlite3_open(fname, &db) != SQLITE_OK)
        return NULL;

    return db;
} // sqlite3FromArchive


static void fail(const char *fmt, ...) NORETURN ISPRINTF(1,2);
static void fail(const char *fmt, ...)
{
    va_list ap;
    fprintf(stderr, "ERROR: ");
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
    fflush(stderr);
    exit(2);
} // fail


#if 0
struct ManifestItem
{
    ManifestItem();
    ~ManifestItem();

    char *domain;
    char *path;
    char *linkTarget;
    uint8 *dataHash;
    char *encryptionKey;
    uint32 mtime;
    uint64 length;
};

ManifestItem::ManifestItem()
    : domain(NULL)
    , path(NULL)
    , linkTarget(NULL)
    , dataHash(NULL)
    , encryptionKey(NULL)
    , mtime(0)
    , length(0)
{
} // ManifestItem::ManifestItem

ManifestItem::~ManifestItem()
{
    delete[] domain;
    delete[] path;
    delete[] linkTarget;
    delete[] (char *) dataHash;
    delete[] encryptionKey;
} // ManifestItem::~ManifestItem

// Read an 8-bit value.
static inline int read8(FILE *io, uint8 *val)
{
    return (fread(val, sizeof (*val), 1, io) == 1);
} // read8

// Read a 16-bit bigendian value.  !!! FIXME: this only works on a littleendian system.
static int read16(FILE *io, uint16 *val)
{
    uint16 x;
    if (fread(&x, sizeof (x), 1, io) != 1)
        return 0;
    *val = (x << 8) | (x >> 8);
    return 1;
} // read16

// Read a 32-bit bigendian value.  !!! FIXME: this only works on a littleendian system.
static int read32(FILE *io, uint32 *val)
{
    uint32 x;
    if (fread(&x, sizeof (x), 1, io) != 1)
        return 0;
    *val = ((x << 24) | ((x << 8) & 0x00FF0000) | ((x >> 8) & 0x0000FF00) | (x >> 24));
    return 1;
} // read32

// Read a 64-bit bigendian value.  !!! FIXME: this only works on a littleendian system.
static int read64(FILE *io, uint64 *val)
{
    uint64 x;
    if (fread(&x, sizeof (x), 1, io) != 1)
        return 0;

    const uint32 lo = static_cast<uint32>(x & 0xFFFFFFFF);
    x >>= 32;
    const uint32 hi = static_cast<uint32>(x & 0xFFFFFFFF);
    x = ((lo << 24) | ((lo << 8) & 0x00FF0000) | ((lo >> 8) & 0x0000FF00) | (lo >> 24));
    x <<= 32;
    x |= ((hi << 24) | ((hi << 8) & 0x00FF0000) | ((hi >> 8) & 0x0000FF00) | (hi >> 24));

    *val = x;
    return 1;
} // read64

static int readstr(FILE *io, char **val)
{
    uint16 len;

    *val = NULL;

    if (!read16(io, &len))
        return 0;
    else if (len == 0xFFFF)
        return 1;

    char *str = new char[len+1];
    if (len && (fread(str, len, 1, io) != 1))
    {
        delete[] str;
        return 0;
    } // if

    str[len] = '\0';
    *val = str;

    return 1;
} // readstr

static int checkManifestSig(FILE *io)
{
    uint8 sig[6];
    if (fread(sig, sizeof (sig), 1, io) != 1)
        return 0;
    return (memcmp(sig, "mbdb\005", 6) == 0);
} // checkManifestSig

static void parseManifest(void)
{
    FILE *manifest;
    if ((manifest = fopen("Manifest.mbdb", "rb")) == NULL)
        fail("can't open Manifest.mbdb: %s", strerror(errno));
    else if (!checkManifestSig(manifest))
        fail("Manifest is corrupt");

    while (1)
    {
        char *str;
        if (!readstr(manifest, &str))
        {
            if (feof(manifest))
                break;  // we're good.
            fail("Manifest is corrupt");
        } // if

        int okay = 1;

        uint8 ui8;
        uint16 ui16;
        uint32 ui32;
        uint8 propcount;

        ManifestItem *item = new ManifestItem;
        item->domain = str;
        okay &= readstr(manifest, &item->path);
        okay &= readstr(manifest, &item->linkTarget);
        okay &= readstr(manifest, &str); item->dataHash = (uint8 *) str;
        okay &= readstr(manifest, &item->encryptionKey);
        okay &= read16(manifest, &ui16);  // mode_t
        okay &= read32(manifest, &ui32);  // inode
        okay &= read32(manifest, &ui32);  // unknown
        okay &= read32(manifest, &ui32);  // uid
        okay &= read32(manifest, &ui32);  // gid
        okay &= read32(manifest, &item->mtime);  // mtime
        okay &= read32(manifest, &ui32);  // atime
        okay &= read32(manifest, &ui32);  // ctime
        okay &= read64(manifest, &item->length);  // length
        okay &= read8(manifest, &ui8);  // protectionclass
        okay &= read8(manifest, &propcount);  // propertyCount

        if (okay)
        {
            for (uint8 i = 0; i < propcount; i++)
            {
                okay &= readstr(manifest, &str);
                delete[] str;
                okay &= readstr(manifest, &str);
                delete[] str;
            } // for
        } // if

        if (!okay)
            fail("Manifest is corrupt");

        printf("ITEM: domain='%s', path='%s', link='%s', ", item->domain, item->path, item->linkTarget);

        if (!item->dataHash)
            printf("hash='%s', ", item->dataHash);
        else
        {
            printf("hash='%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x', ",
                    (unsigned int) item->dataHash[0], (unsigned int) item->dataHash[1], (unsigned int) item->dataHash[2],
                    (unsigned int) item->dataHash[3], (unsigned int) item->dataHash[4], (unsigned int) item->dataHash[5],
                    (unsigned int) item->dataHash[6], (unsigned int) item->dataHash[7], (unsigned int) item->dataHash[8],
                    (unsigned int) item->dataHash[9], (unsigned int) item->dataHash[10], (unsigned int) item->dataHash[11],
                    (unsigned int) item->dataHash[12], (unsigned int) item->dataHash[13], (unsigned int) item->dataHash[14],
                    (unsigned int) item->dataHash[15], (unsigned int) item->dataHash[16], (unsigned int) item->dataHash[17],
                    (unsigned int) item->dataHash[18], (unsigned int) item->dataHash[19]);
        } // else
        printf("key='%s', mtime=%u, len=%llu\n", item->encryptionKey, (unsigned int) item->mtime, (unsigned long long) item->length);

        delete item;
    } // while

    fclose(manifest);
} // parseManifest
#endif

static void usageAndQuit(const char *argv0)
{
    fprintf(stderr, "USAGE: %s <backupdir>\n", argv0);
    exit(1);
} // usageAndQuit

int main(int argc, char **argv)
{
    if (argc != 2)
        usageAndQuit(argv[0]);
    else if (chdir(argv[1]) == -1)
        fail("can't chdir() to %s: %s", argv[1], strerror(errno));
    //parseManifest();

    sqlite3 *db = sqlite3FromArchive("HomeDomain", "Library/SMS/sms.db");
    if (!db)
        fail("can't open sms.db");

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, "select text from message order by ROWID;", -1, &stmt, NULL) != SQLITE_OK)
        fail("can't prepare SELECT statement");
    else if (sqlite3_reset(stmt) != SQLITE_OK)
        fail("can't reset SELECT statement");
    //else if (sqlite3_bind_text(stmt, 1, path, -1, SQLITE_STATIC) != SQLITE_OK)
    //    fail("can't bind SELECT statement");

    while (true)
    {
        const int rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW)
            printf("%s\n", sqlite3_column_text(stmt, 0));
        else if (rc == SQLITE_DONE)
            break;
        else
            fail("SELECT statement reported error");
    } // while

    sqlite3_reset(stmt);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
} // main

// end of ios-msgdump.cpp ...

