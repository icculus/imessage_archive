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

struct ManifestItem
{
    ManifestItem();
    ~ManifestItem();

    char *domain;
    char *path;
    char *linkTarget;
    uint8_t *dataHash;
    char *encryptionKey;
    uint32_t mtime;
    uint64_t length;
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

// Read an 8-bit value.
static inline int read8(FILE *io, uint8_t *val)
{
    return (fread(val, sizeof (*val), 1, io) == 1);
} // read8

// Read a 16-bit bigendian value.  !!! FIXME: this only works on a littleendian system.
static int read16(FILE *io, uint16_t *val)
{
    uint16_t x;
    if (fread(&x, sizeof (x), 1, io) != 1)
        return 0;
    *val = (x << 8) | (x >> 8);
    return 1;
} // read16

// Read a 32-bit bigendian value.  !!! FIXME: this only works on a littleendian system.
static int read32(FILE *io, uint32_t *val)
{
    uint32_t x;
    if (fread(&x, sizeof (x), 1, io) != 1)
        return 0;
    *val = ((x << 24) | ((x << 8) & 0x00FF0000) | ((x >> 8) & 0x0000FF00) | (x >> 24));
    return 1;
} // read32

// Read a 64-bit bigendian value.  !!! FIXME: this only works on a littleendian system.
static int read64(FILE *io, uint64_t *val)
{
    uint64_t x;
    if (fread(&x, sizeof (x), 1, io) != 1)
        return 0;

    const uint32_t lo = static_cast<uint32_t>(x & 0xFFFFFFFF);
    x >>= 32;
    const uint32_t hi = static_cast<uint32_t>(x & 0xFFFFFFFF);
    x = ((lo << 24) | ((lo << 8) & 0x00FF0000) | ((lo >> 8) & 0x0000FF00) | (lo >> 24));
    x <<= 32;
    x |= ((hi << 24) | ((hi << 8) & 0x00FF0000) | ((hi >> 8) & 0x0000FF00) | (hi >> 24));

    *val = x;
    return 1;
} // read64

static int readstr(FILE *io, char **val)
{
    uint16_t len;

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
    uint8_t sig[6];
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

        uint8_t ui8;
        uint16_t ui16;
        uint32_t ui32;
        uint8_t propcount;

        ManifestItem *item = new ManifestItem;
        item->domain = str;
        okay &= readstr(manifest, &item->path);
        okay &= readstr(manifest, &item->linkTarget);
        okay &= readstr(manifest, &str); item->dataHash = (uint8_t *) str;
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
            for (uint8_t i = 0; i < propcount; i++)
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
            printf("hash='%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x', ",
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
    parseManifest();

    return 0;
} // main

// end of ios-msgdump.cpp ...

