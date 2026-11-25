#import <Foundation/Foundation.h>
#import <NetFS/NetFS.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <os/log.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sysexits.h>
#include <time.h>
#include <unistd.h>

enum {
    exitRefusedMount = 79,
    exitStateMismatch = 80,
    retryLimit = 2,
    retryWindow = 15 * 60,
    retryBackoff = 2 * 60,
};

static const char *const authHold = "auth-hold";
static const char *const refusedMount = "refused-mount";
static const char *const networkRetry = "network-retry";

typedef struct {
    int64_t windowStart;
    int64_t lastFailure;
    int64_t attempts;
} RetryState;

// Use the kernel's cached inventory so a stalled SMB request does not block
// the decision or cause another mount attempt against an existing volume.
static int checkMount(const char *point, const char *source) {
    struct statfs *mounts;
    int count = getmntinfo(&mounts, MNT_NOWAIT);
    if (count <= 0) {
        return EX_OSERR;
    }
    BOOL found = NO;
    for (int i = 0; i < count; i++) {
        const struct statfs *entry = &mounts[i];
        BOOL target = strcmp(entry->f_mntonname, point) == 0;
        BOOL expected = strcmp(entry->f_fstypename, "smbfs") == 0 &&
                        strcmp(entry->f_mntfromname, source) == 0;
        if (target != expected || (target && found)) {
            return EX_CONFIG;
        }
        found |= target;
    }
    return found ? EX_OK : EX_UNAVAILABLE;
}

static BOOL validAbsolutePath(NSString *path) {
    if (![path hasPrefix:@"/"]) {
        return NO;
    }
    for (NSString *component in [[path substringFromIndex:1] componentsSeparatedByString:@"/"]) {
        if (component.length == 0 || [component isEqualToString:@"."] || [component isEqualToString:@".."]) {
            return NO;
        }
    }
    return YES;
}

static BOOL sameNode(struct stat first, struct stat second) {
    return first.st_dev == second.st_dev && first.st_ino == second.st_ino && first.st_uid == second.st_uid;
}

static BOOL directoryAtPath(int fd, const char *path) {
    char resolved[PATH_MAX];
    struct stat byFD, byPath;
    return realpath(path, resolved) != NULL && strcmp(resolved, path) == 0 &&
           fstat(fd, &byFD) == 0 && byFD.st_uid == geteuid() &&
           lstat(path, &byPath) == 0 && S_ISDIR(byPath.st_mode) && sameNode(byFD, byPath);
}

static int openLocalDirectory(const char *path) {
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW_ANY | O_CLOEXEC);
    if (fd < 0) {
        return -1;
    }
    struct statfs filesystem;
    if (fstatfs(fd, &filesystem) != 0 || !(filesystem.f_flags & MNT_LOCAL) || !directoryAtPath(fd, path)) {
        close(fd);
        return -1;
    }
    return fd;
}

static BOOL emptyDirectory(DIR *dir) {
    rewinddir(dir);
    errno = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
            return NO;
        }
    }
    return errno == 0;
}

static BOOL holdAtPath(int directory, int hold) {
    struct stat byFD, byPath;
    return fstat(hold, &byFD) == 0 &&
           fstatat(directory, authHold, &byPath, AT_SYMLINK_NOFOLLOW) == 0 &&
           S_ISREG(byPath.st_mode) && sameNode(byFD, byPath);
}

static BOOL recoveryDirectoryAtPath(int directory, const char *path) {
    struct stat info;
    return fstat(directory, &info) == 0 && (info.st_mode & 0777) == 0700 && directoryAtPath(directory, path);
}

static int checkHold(int directory, const char *path) {
    struct stat info;
    int status = EX_OK;
    if (fstatat(directory, refusedMount, &info, AT_SYMLINK_NOFOLLOW) == 0) {
        status = exitRefusedMount;
    } else if (errno != ENOENT) {
        status = EX_CANTCREAT;
    } else if (fstatat(directory, authHold, &info, AT_SYMLINK_NOFOLLOW) == 0) {
        status = EX_NOPERM;
    } else if (errno != ENOENT) {
        status = EX_CANTCREAT;
    }
    return recoveryDirectoryAtPath(directory, path) ? status : exitStateMismatch;
}

static BOOL syncState(int file, int directory) {
    return fsync(file) == 0 && fsync(directory) == 0 && fcntl(file, F_FULLFSYNC) == 0;
}

static int checkRetryBudget(int directory, RetryState *retry) {
    int fd = openat(directory, networkRetry, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0) {
        return errno == ENOENT ? EX_OK : EX_CANTCREAT;
    }
    struct stat info;
    BOOL valid = fstat(fd, &info) == 0 && S_ISREG(info.st_mode) &&
                 info.st_size == sizeof(*retry) && read(fd, retry, sizeof(*retry)) == sizeof(*retry);
    close(fd);
    if (!valid || retry->windowStart <= 0 || retry->lastFailure < retry->windowStart ||
        retry->attempts < 1 || retry->attempts > retryLimit) {
        return EX_CANTCREAT;
    }
    time_t now = time(NULL);
    if (now <= 0) {
        return EX_CANTCREAT;
    }
    if (now - retry->lastFailure < retryBackoff ||
        (now - retry->windowStart < retryWindow && retry->attempts == retryLimit)) {
        os_log(OS_LOG_DEFAULT, "Mycelium NAS remount deferred by network retry budget");
        return EX_TEMPFAIL;
    }
    return EX_OK;
}

static BOOL recordNetworkFailure(int directory, RetryState retry, int result) {
    time_t now = time(NULL);
    if (now <= 0 || now < retry.lastFailure) {
        return NO;
    }
    if (retry.attempts == 0 || now - retry.windowStart >= retryWindow) {
        retry = (RetryState){.windowStart = now};
    }
    retry.lastFailure = now;
    retry.attempts++;
    int fd = openat(directory, networkRetry, O_WRONLY | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (fd < 0) {
        return NO;
    }
    // The synced attempt hold stays in place if this record cannot be saved.
    BOOL saved = write(fd, &retry, sizeof(retry)) == sizeof(retry) && syncState(fd, directory);
    close(fd);
    if (saved) {
        os_log(OS_LOG_DEFAULT, "Mycelium NAS remount NetFS status %{public}d used network retry budget %{public}lld/%{public}d",
               result, (long long)retry.attempts, retryLimit);
    }
    return saved;
}

// NetFS returns a status without the SMB phase. A transport error is not a
// rejected login: the saved credential is unchanged, so retrying cannot add a
// failed-login count. Transport errors therefore retry automatically, bounded
// by the per-window budget and backoff; rejected or unknown results hold.
static BOOL networkError(int status) {
    switch (status) {
        case ENETDOWN:
        case ENETUNREACH:
        case EHOSTDOWN:
        case EHOSTUNREACH:
        case ECONNREFUSED:
        case ENOTCONN:
        case EPIPE:
        case ETIMEDOUT:
            return YES;
        default:
            return NO;
    }
}

static int mountShare(NSURL *url, NSString *point, const char *source, DIR *target, int *netfsStatus) {
    const char *path = point.fileSystemRepresentation;
    NSURL *mountURL = [NSURL fileURLWithPath:point isDirectory:YES];
    // NetAuth can reuse the saved login in this GUI session. Supply no password
    // and request no authentication UI.
    NSMutableDictionary *session = [@{(__bridge NSString *)kNAUIOptionKey:
                                       (__bridge NSString *)kNAUIOptionNoUI} mutableCopy];
    NSMutableDictionary *options = [@{(__bridge NSString *)kNetFSMountAtMountDirKey: @YES} mutableCopy];
    CFArrayRef mounted = NULL;

    int result = NetFSMountURLSync((__bridge CFURLRef)url,
                                  (__bridge CFURLRef)mountURL,
                                  NULL, NULL, (__bridge CFMutableDictionaryRef)session,
                                  (__bridge CFMutableDictionaryRef)options, &mounted);
    *netfsStatus = result;
    if (mounted != NULL) {
        CFRelease(mounted);
    }

    // The descriptor still refers to the underlying local directory. Avoid
    // following the pathname into a fresh SMB mount to check for local changes.
    int targetFD = dirfd(target);
    char underlyingPath[PATH_MAX];
    struct stat underlying;
    if (fcntl(targetFD, F_GETPATH, underlyingPath) != 0 || strcmp(underlyingPath, path) != 0 ||
        fstat(targetFD, &underlying) != 0 || underlying.st_uid != geteuid() || !emptyDirectory(target)) {
        os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount target changed during NetFS");
        return EX_CONFIG;
    }
    int state = checkMount(path, source);
    if (result == 0 && state == EX_OK) {
        return EX_OK;
    }
    if (result == 0 || state != EX_UNAVAILABLE) {
        os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount returned unexpected mount inventory, NetFS status %{public}d", result);
        return EX_CONFIG;
    }
    os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount failed, NetFS status %{public}d", result);
    return EX_NOPERM;
}

static int mountWithHold(NSURL *url, NSString *point, const char *source, DIR *target,
                         int directory, const char *directoryPath) {
    RetryState retry = {0};
    int status = checkRetryBudget(directory, &retry);
    if (status != EX_OK) {
        return status;
    }

    int hold = openat(directory, authHold, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (hold < 0) {
        return errno == EEXIST ? EX_NOPERM : EX_CANTCREAT;
    }

    // A stopped process or any uncertain outcome leaves an attempt hold.
    // Persist the file and the directory entry before NetFS can authenticate.
    BOOL called = NO;
    int netfsStatus = 0;
    status = EX_CANTCREAT;
    if (syncState(hold, directory)) {
        const char *path = point.fileSystemRepresentation;
        status = checkMount(path, source);
        if (status == EX_UNAVAILABLE) {
            if (!emptyDirectory(target) || !directoryAtPath(dirfd(target), path)) {
                status = EX_CONFIG;
            } else if (!holdAtPath(directory, hold)) {
                status = EX_CANTCREAT;
            } else if (!recoveryDirectoryAtPath(directory, directoryPath)) {
                status = exitStateMismatch;
            } else {
                called = YES;
                status = mountShare(url, point, source, target, &netfsStatus);
            }
        }
    }

    if (!called) {
        os_log(OS_LOG_DEFAULT, "Mycelium NAS remount skipped NetFS before call, helper status %{public}d", status);
    } else if (status == EX_CONFIG) {
        // A rename keeps either the durable attempt hold or the refusal
        // visible. A later exact mount cannot clear either record.
        if (!holdAtPath(directory, hold) || renameat(directory, authHold, directory, refusedMount) != 0 ||
            fsync(directory) != 0 || fcntl(hold, F_FULLFSYNC) != 0) {
            status = EX_CANTCREAT;
        }
    }
    BOOL transient = NO;
    if (called && networkError(netfsStatus)) {
        if (!recordNetworkFailure(directory, retry, netfsStatus)) {
            status = EX_CANTCREAT;
        } else if (status == EX_NOPERM) {
            transient = YES;
            status = EX_TEMPFAIL;
        }
    }

    if (!called || status == EX_OK || transient) {
        if (!holdAtPath(directory, hold) || unlinkat(directory, authHold, 0) != 0 || fsync(directory) != 0) {
            status = EX_CANTCREAT;
        }
    }
    close(hold);
    if (!recoveryDirectoryAtPath(directory, directoryPath)) {
        return exitStateMismatch;
    }
    if (called && status == EX_OK) {
        os_log(OS_LOG_DEFAULT, "Mycelium NAS remount restored the expected SMB volume");
    }
    return status;
}

static int openRecoveryDirectory(const char *path, const char *identity) {
    int directory = openLocalDirectory(path);
    if (directory < 0) {
        return -1;
    }
    struct stat info;
    if (fstat(directory, &info) == 0 && (info.st_mode & 0777) == 0700) {
        char actualIdentity[64];
        snprintf(actualIdentity, sizeof(actualIdentity), "%u:%llu",
                 (unsigned int)info.st_dev, (unsigned long long)info.st_ino);
        if (strcmp(actualIdentity, identity) == 0) {
            return directory;
        }
    }
    close(directory);
    return -1;
}

static int checkOrMount(NSURL *url, NSString *point, const char *source, const char *directoryPath,
                        int directory, BOOL checkOnly) {
    const char *path = point.fileSystemRepresentation;
    int state = checkMount(path, source);
    int status = checkHold(directory, directoryPath);
    if (status != EX_OK) {
        return status;
    }
    if (state != EX_UNAVAILABLE) {
        if (state != EX_OK) {
            os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount refused mount inventory");
        }
        return state;
    }

    int fd = openLocalDirectory(path);
    DIR *target = fd < 0 ? NULL : fdopendir(fd);
    if (target == NULL && fd >= 0) {
        close(fd);
    }
    status = EX_CONFIG;
    if (target != NULL && emptyDirectory(target)) {
        status = checkOnly ? EX_UNAVAILABLE : mountWithHold(url, point, source, target, directory, directoryPath);
    } else {
        os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount refused the target directory");
    }
    if (target != NULL) {
        closedir(target);
    }
    return status;
}

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        BOOL checkOnly = argc > 1 && strcmp(argv[1], "--check") == 0;
        int first = checkOnly ? 2 : 1;
        if (argc != first + 5) {
            return EX_USAGE;
        }
        NSURLComponents *components = [NSURLComponents componentsWithString:@(argv[first])];
        NSURL *url = components.URL;
        NSString *point = @(argv[first + 1]);
        const char *source = argv[first + 2];
        NSString *share = [components.path hasPrefix:@"/"] ? [components.path substringFromIndex:1] : @"";
        if (url == nil || ![components.scheme.lowercaseString isEqualToString:@"smb"] ||
            components.host.length == 0 || components.user.length == 0 || components.password != nil ||
            share.length == 0 || [share isEqualToString:@"."] || [share isEqualToString:@".."] ||
            [share rangeOfCharacterFromSet:[NSCharacterSet characterSetWithCharactersInString:@"/\\"]].location != NSNotFound ||
            [share rangeOfCharacterFromSet:NSCharacterSet.controlCharacterSet].location != NSNotFound ||
            components.query != nil || components.fragment != nil ||
            !validAbsolutePath(point) || strncmp(source, "//", 2) != 0) {
            return EX_USAGE;
        }
        // The SMB mount-table source is the escaped URL without its scheme.
        // Compare the actual URL passed to NetFS, including user, host and port.
        if (strcmp([url.absoluteString substringFromIndex:4].UTF8String, source) != 0) {
            os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount URL differs from the expected SMB source");
            return EX_CONFIG;
        }
        NSString *statePath = @(argv[first + 3]);
        if (!validAbsolutePath(statePath) ||
            [statePath hasPrefix:[point stringByAppendingString:@"/"]] || [statePath isEqualToString:point]) {
            return EX_USAGE;
        }

        const char *directoryPath = statePath.fileSystemRepresentation;
        int directory = openRecoveryDirectory(directoryPath, argv[first + 4]);
        int status = exitStateMismatch;
        if (directory >= 0) {
            status = checkOrMount(url, point, source, directoryPath, directory, checkOnly);
            close(directory);
        }
        switch (status) {
            case exitRefusedMount:
                os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount refused mount needs operator inspection");
                break;
            case exitStateMismatch:
                os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount recovery directory identity or mode mismatch");
                break;
            case EX_NOPERM:
                os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount authentication attempt held");
                break;
            case EX_CANTCREAT:
                os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount could not read or persist attempt state");
                break;
        }
        return status;
    }
}
