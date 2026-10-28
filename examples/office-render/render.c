// One document per process; build with the installed LibreOfficeKit headers.
#define _GNU_SOURCE
#include <stdbool.h>
#include <LibreOfficeKit/LibreOfficeKitInit.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static char *pending_output;

static _Noreturn void finish(int status)
{
    if (pending_output)
        unlink(pending_output);
    if (fflush(NULL) != 0)
        status = 1;
    // LO 24.2's exit-time global destructors crash after successful kit
    // destruction. Never run them: success requires save, destroy, verify,
    // fsync, close and rename below. Failures retain their nonzero status.
    _exit(status);
}

static _Noreturn void fail(const char *message)
{
    fprintf(stderr, "office-render: %s\n", message);
    finish(1);
}

static char *join(const char *left, const char *right)
{
    char *path;
    if (asprintf(&path, "%s/%s", left, right) < 0)
        fail("out of memory");
    return path;
}

static char *file_uri(const char *path)
{
    const char hex[] = "0123456789ABCDEF";
    char *uri = malloc(8 + 3 * strlen(path));
    if (!uri)
        fail("out of memory");
    memcpy(uri, "file://", 7);
    char *out = uri + 7;
    for (const unsigned char *p = (const unsigned char *)path; *p; ++p) {
        if ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
            (*p >= '0' && *p <= '9') || strchr("/-._~", *p)) {
            *out++ = *p;
        } else {
            *out++ = '%';
            *out++ = hex[*p >> 4];
            *out++ = hex[*p & 15];
        }
    }
    *out = 0;
    return uri;
}

static void set_env(const char *key, const char *value)
{
    if (setenv(key, value, 1) != 0)
        fail("cannot set private environment");
}

static char *private_dir(const char *root, const char *name)
{
    char *path = join(root, name);
    if (mkdir(path, 0700) != 0)
        fail("cannot create private directory");
    return path;
}

static void xml_text(FILE *file, const char *text)
{
    for (; *text; ++text) {
        switch (*text) {
        case '&': fputs("&amp;", file); break;
        case '<': fputs("&lt;", file); break;
        case '>': fputs("&gt;", file); break;
        default: fputc(*text, file);
        }
    }
}

static char *private_environment(const char *scratch)
{
    char *root = join(scratch, "office-XXXXXX");
    if (!mkdtemp(root))
        fail("cannot create private Office scratch");
    set_env("HOME", private_dir(root, "home"));
    set_env("XDG_RUNTIME_DIR", private_dir(root, "runtime"));
    set_env("XDG_CONFIG_HOME", private_dir(root, "config"));
    set_env("XDG_DATA_HOME", private_dir(root, "data"));
    char *cache = private_dir(root, "cache");
    set_env("XDG_CACHE_HOME", cache);
    char *font_cache = private_dir(cache, "fontconfig");
    char *config = join(root, "fonts.conf");
    FILE *file = fopen(config, "wx");
    if (!file)
        fail("cannot create private fontconfig");
    fputs("<fontconfig><dir>/usr/share/fonts</dir>"
          "<dir>/usr/local/share/fonts</dir><cachedir>", file);
    xml_text(file, font_cache);
    fputs("</cachedir></fontconfig>\n", file);
    bool config_failed = ferror(file);
    if (fclose(file) != 0 || config_failed)
        fail("cannot write private fontconfig");
    set_env("FONTCONFIG_FILE", config);
    set_env("FONTCONFIG_PATH", root);
    set_env("SAL_USE_VCLPLUGIN", "svp");
    char *profile = file_uri(private_dir(root, "profile"));

    // Optional distribution registry staged as ordinary job inputs. This
    // avoids granting /etc/libreoffice when share/registry symlinks there.
    const char *registry = getenv("MYCE_OFFICE_REGISTRY");
    if (registry) {
        char *path = realpath(registry, NULL), *layers;
        if (!path || asprintf(&layers,
                "xcsxcu:%s res:%s user:!%s/user/registrymodifications.xcu",
                file_uri(path), file_uri(path), profile) < 0)
            fail("cannot resolve staged Office registry");
        set_env("CONFIGURATION_LAYERS", layers);
    }
    return profile;
}

static void verify_and_flush(const char *path)
{
    int fd = open(path, O_RDWR | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0)
        fail("cannot open rendered PDF");
    struct stat info;
    char header[5], tail[1025] = {0};
    if (fstat(fd, &info) != 0 || !S_ISREG(info.st_mode) || info.st_size < 12 ||
        pread(fd, header, sizeof(header), 0) != sizeof(header) ||
        memcmp(header, "%PDF-", sizeof(header)) != 0)
        fail("rendered output is not a PDF");
    size_t length = info.st_size < 1024 ? (size_t)info.st_size : 1024;
    if (pread(fd, tail, length, info.st_size - length) != (ssize_t)length ||
        !memmem(tail, length, "%%EOF", 5))
        fail("rendered PDF is incomplete");
    if (fchmod(fd, 0600) != 0 || fsync(fd) != 0)
        fail("cannot flush rendered PDF");
    if (close(fd) != 0)
        fail("cannot close rendered PDF");
}

int main(int argc, char **argv)
{
    if (argc != 4) {
        fprintf(stderr, "usage: office-render PROGRAM_DIRECTORY INPUT OUTPUT.pdf\n"
                        "requires MYCE_SCRATCH_DIR; optional MYCE_OFFICE_REGISTRY\n");
        finish(2);
    }
    umask(077);
    const char *scratch_env = getenv("MYCE_SCRATCH_DIR");
    char *scratch = scratch_env ? realpath(scratch_env, NULL) : NULL;
    char *program = realpath(argv[1], NULL), *input = realpath(argv[2], NULL);
    if (!scratch || !program || !input)
        fail("program, input and MYCE_SCRATCH_DIR must exist");
    char *profile = private_environment(scratch);

    // Render into an owned file beside the destination, so a failed render
    // never publishes a partial PDF and the final rename is atomic.
    if (asprintf(&pending_output, "%s.XXXXXX", argv[3]) < 0)
        fail("out of memory");
    int fd = mkstemp(pending_output);
    if (fd < 0)
        fail("cannot create output temporary file");
    if (close(fd) != 0)
        fail("cannot close output temporary file");
    char *output = realpath(pending_output, NULL);
    if (!output)
        fail("cannot resolve output temporary file");

    // Canonical loader; LOK initializes RequestHandler with IPC disabled.
    LibreOfficeKit *kit = lok_init_2(program, profile);
    if (!kit)
        fail("LibreOfficeKit initialization failed");
    LibreOfficeKitDocument *doc = kit->pClass->documentLoad(kit, file_uri(input));
    bool saved = doc && doc->pClass->saveAs(doc, file_uri(output), "pdf", NULL);
    if (!saved) {
        char *error = kit->pClass->getError(kit);
        fprintf(stderr, "office-render: conversion failed: %s\n", error ? error : "unknown error");
        if (error)
            kit->pClass->freeError(error);
    }
    if (doc)
        doc->pClass->destroy(doc);
    kit->pClass->destroy(kit);
    if (!saved)
        finish(1);

    verify_and_flush(output);
    if (rename(pending_output, argv[3]) != 0)
        fail("cannot publish rendered PDF");
    pending_output = NULL;
    fprintf(stderr, "office-render: PDF saved, Office destroyed, verified and flushed\n");
    finish(0);
}
