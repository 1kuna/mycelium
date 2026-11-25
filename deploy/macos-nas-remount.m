#import <Foundation/Foundation.h>
#import <NetFS/NetFS.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <os/log.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sysexits.h>
#include <unistd.h>

// Use the kernel's cached inventory so a stalled SMB request does not block
// the decision or cause another mount attempt against an existing volume.
static int checkMount(NSString *point, const char *source) {
    struct statfs *mounts;
    int count = getmntinfo(&mounts, MNT_NOWAIT);
    if (count <= 0) {
        return EX_OSERR;
    }
    BOOL found = NO;
    for (int i = 0; i < count; i++) {
        const struct statfs *entry = &mounts[i];
        BOOL target = strcmp(entry->f_mntonname, point.fileSystemRepresentation) == 0;
        BOOL expected = strcmp(entry->f_fstypename, "smbfs") == 0 &&
                        strcmp(entry->f_mntfromname, source) == 0;
        if (target != expected) {
            return EX_CONFIG;
        }
        found |= target;
    }
    return found ? EX_OK : EX_UNAVAILABLE;
}

static BOOL emptyLocalDirectory(NSString *point) {
    int fd = open(point.fileSystemRepresentation, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    if (fd < 0) {
        return NO;
    }
    struct statfs filesystem;
    struct stat directory;
    if (fstatfs(fd, &filesystem) != 0 || !(filesystem.f_flags & MNT_LOCAL) ||
        fstat(fd, &directory) != 0 || directory.st_uid != geteuid()) {
        close(fd);
        return NO;
    }
    DIR *dir = fdopendir(fd);
    if (dir == NULL) {
        close(fd);
        return NO;
    }
    BOOL empty = YES;
    errno = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
            empty = NO;
            break;
        }
    }
    empty = empty && errno == 0;
    closedir(dir);
    return empty;
}

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        BOOL checkOnly = argc == 5 && strcmp(argv[1], "--check") == 0;
        if (argc != 5) {
            return EX_USAGE;
        }
        int first = checkOnly ? 2 : 1;
        NSURLComponents *url = [NSURLComponents componentsWithString:@(argv[first])];
        NSString *point = @(argv[first + 1]);
        const char *source = argv[first + 2];
        NSString *share = [url.path hasPrefix:@"/"] ? [url.path substringFromIndex:1] : @"";
        if (![url.scheme.lowercaseString isEqualToString:@"smb"] ||
            url.host.length == 0 || url.user.length == 0 || url.password != nil ||
            share.length == 0 || [share containsString:@"/"] ||
            url.query != nil || url.fragment != nil ||
            !point.isAbsolutePath || [point isEqualToString:@"/"] ||
            ![point isEqualToString:point.stringByStandardizingPath] ||
            strncmp(source, "//", 2) != 0) {
            return EX_USAGE;
        }
        int state = checkMount(point, source);
        if (checkOnly || state == EX_OK) {
            return state;
        }
        if (state != EX_UNAVAILABLE || !emptyLocalDirectory(point)) {
            os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount refused mount inventory or target directory");
            return state == EX_UNAVAILABLE ? EX_CONFIG : state;
        }
        const char *authHold = argv[first + 3];
        struct stat hold;
        if (authHold[0] != '/') {
            return EX_USAGE;
        }
        if (lstat(authHold, &hold) == 0) {
            return EX_NOPERM;
        }
        if (errno != ENOENT) {
            return EX_CANTCREAT;
        }

        // NetAuth decides whether this GUI session can reuse saved credentials.
        // The helper supplies no password, reads no credential and asks for no UI.
        NSMutableDictionary *session = [@{(__bridge NSString *)kNAUIOptionKey:
                                           (__bridge NSString *)kNAUIOptionNoUI} mutableCopy];
        NSMutableDictionary *options = [@{(__bridge NSString *)kNetFSMountAtMountDirKey: @YES} mutableCopy];
        CFArrayRef mounted = NULL;
        int result = NetFSMountURLSync((__bridge CFURLRef)url.URL,
                                      (__bridge CFURLRef)[NSURL fileURLWithPath:point isDirectory:YES],
                                      NULL, NULL, (__bridge CFMutableDictionaryRef)session,
                                      (__bridge CFMutableDictionaryRef)options, &mounted);
        if (mounted != NULL) {
            CFRelease(mounted);
        }
        if (result != 0) {
            os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount failed, NetFS status %{public}d", result);
            // An explicit refusal is different from an interrupted connection.
            // Keep the refusal across timer ticks to avoid repeated logon attempts.
            switch (result) {
                case EAUTH:
                case ENEEDAUTH:
                case EACCES:
                case EPERM:
                case ENETFSPWDNEEDSCHANGE:
                case ENETFSPWDPOLICY:
                case ENETFSACCOUNTRESTRICTED: {
                    int fd = open(authHold, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
                    if (fd < 0) {
                        return errno == EEXIST ? EX_NOPERM : EX_CANTCREAT;
                    }
                    close(fd);
                    return EX_NOPERM;
                }
            }
            return EX_TEMPFAIL;
        }
        if (checkMount(point, source) != EX_OK) {
            os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount returned without the expected SMB volume");
            return EX_CONFIG;
        }
        os_log(OS_LOG_DEFAULT, "Mycelium NAS remount restored the expected SMB volume");
        return EX_OK;
    }
}
