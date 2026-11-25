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

static BOOL holdAtPath(int directory, const char *directoryPath, int hold, const char *name) {
    struct stat byFD, byPath;
    return directoryAtPath(directory, directoryPath) &&
           fstat(hold, &byFD) == 0 &&
           fstatat(directory, name, &byPath, AT_SYMLINK_NOFOLLOW) == 0 &&
           S_ISREG(byPath.st_mode) && sameNode(byFD, byPath);
}

static BOOL transientNetworkError(int status) {
    switch (status) {
        case ENETDOWN:
        case ENETUNREACH:
        case EHOSTDOWN:
        case EHOSTUNREACH:
        case ETIMEDOUT:
        case ECONNREFUSED:
            return YES;
        default:
            return NO;
    }
}

static int mountIfUnchanged(NSURL *url, NSString *point, const char *source, DIR *target) {
    const char *path = point.fileSystemRepresentation;
    NSURL *mountURL = [NSURL fileURLWithPath:point isDirectory:YES];
    // NetAuth can reuse the saved login in this GUI session. Supply no password
    // and request no authentication UI.
    NSMutableDictionary *session = [@{(__bridge NSString *)kNAUIOptionKey:
                                       (__bridge NSString *)kNAUIOptionNoUI} mutableCopy];
    NSMutableDictionary *options = [@{(__bridge NSString *)kNetFSMountAtMountDirKey: @YES} mutableCopy];
    CFArrayRef mounted = NULL;

    // Keep the original local directory open. Recheck its contents and the
    // canonical path immediately before handing the path to NetFS.
    int targetFD = dirfd(target);
    if (!emptyDirectory(target) || !directoryAtPath(targetFD, path)) {
        os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount target changed before NetFS");
        return EX_CONFIG;
    }
    int result = NetFSMountURLSync((__bridge CFURLRef)url,
                                  (__bridge CFURLRef)mountURL,
                                  NULL, NULL, (__bridge CFMutableDictionaryRef)session,
                                  (__bridge CFMutableDictionaryRef)options, &mounted);
    if (mounted != NULL) {
        CFRelease(mounted);
    }

    // The descriptor still refers to the underlying local directory. Avoid
    // following the pathname into a fresh SMB mount to check for local changes.
    char underlyingPath[PATH_MAX];
    struct stat underlying;
    if (fcntl(targetFD, F_GETPATH, underlyingPath) != 0 || strcmp(underlyingPath, path) != 0 ||
        fstat(targetFD, &underlying) != 0 || underlying.st_uid != geteuid() || !emptyDirectory(target)) {
        os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount target changed during NetFS");
        return EX_CONFIG;
    }
    int state = checkMount(path, source);
    if (result == 0 && state == EX_OK) {
        os_log(OS_LOG_DEFAULT, "Mycelium NAS remount restored the expected SMB volume");
        return EX_OK;
    }
    if (result == 0 || state != EX_UNAVAILABLE) {
        os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount returned unexpected mount inventory, NetFS status %{public}d", result);
        return EX_CONFIG;
    }
    os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount failed, NetFS status %{public}d", result);
    return transientNetworkError(result) ? EX_TEMPFAIL : EX_NOPERM;
}

static int mountWithHold(NSURL *url, NSString *point, const char *source, DIR *target, NSString *holdPath) {
    NSString *parent = holdPath.stringByDeletingLastPathComponent;
    const char *directoryPath = parent.fileSystemRepresentation;
    const char *name = holdPath.lastPathComponent.fileSystemRepresentation;
    int directory = openLocalDirectory(directoryPath);
    if (directory < 0) {
        return EX_CANTCREAT;
    }
    struct stat info;
    if (fstat(directory, &info) != 0 || (info.st_mode & 0777) != 0700) {
        close(directory);
        return EX_CANTCREAT;
    }
    int hold = openat(directory, name, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (hold < 0) {
        int status = errno == EEXIST ? EX_NOPERM : EX_CANTCREAT;
        close(directory);
        return status;
    }

    // A stopped process or any uncertain outcome leaves the attempt held.
    // Persist the file and the directory entry before NetFS can authenticate.
    int status = EX_CANTCREAT;
    if (fsync(hold) == 0 && fsync(directory) == 0 && fcntl(hold, F_FULLFSYNC) == 0) {
        status = checkMount(point.fileSystemRepresentation, source);
        if (status == EX_UNAVAILABLE) {
            status = holdAtPath(directory, directoryPath, hold, name) ?
                     mountIfUnchanged(url, point, source, target) : EX_CANTCREAT;
        } else if (status != EX_OK) {
            os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount inventory changed before NetFS");
        }
        if (status == EX_OK || status == EX_TEMPFAIL) {
            if (!holdAtPath(directory, directoryPath, hold, name) ||
                unlinkat(directory, name, 0) != 0 || fsync(directory) != 0) {
                status = EX_CANTCREAT;
            }
        }
    }
    close(hold);
    close(directory);
    return status;
}

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        if (argc != 5) {
            return EX_USAGE;
        }
        BOOL checkOnly = strcmp(argv[1], "--check") == 0;
        int first = checkOnly ? 2 : 1;
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
        NSString *holdPath = checkOnly ? nil : @(argv[first + 3]);
        if (!checkOnly && (!validAbsolutePath(holdPath) ||
                           [holdPath.stringByDeletingLastPathComponent isEqualToString:@"/"] ||
                           [holdPath hasPrefix:[point stringByAppendingString:@"/"]] ||
                           [holdPath isEqualToString:point])) {
            return EX_USAGE;
        }

        const char *path = point.fileSystemRepresentation;
        int state = checkMount(path, source);
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
        int status = EX_CONFIG;
        if (target != NULL && emptyDirectory(target)) {
            status = checkOnly ? EX_UNAVAILABLE : mountWithHold(url, point, source, target, holdPath);
        } else {
            os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount refused the target directory");
        }
        if (target != NULL) {
            closedir(target);
        }
        if (status == EX_CANTCREAT) {
            os_log_error(OS_LOG_DEFAULT, "Mycelium NAS remount could not persist attempt state");
        }
        return status;
    }
}
