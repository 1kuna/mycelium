// Replace OS calls with fixtures. No SMB or mount syscall runs in these tests.
#include <time.h>
#include <unistd.h>
@class NSString;
static NSString *root;
time_t fixtureTime(time_t *result);
static int fixtureFsync(int fd);
#define NETFS_MOUNT_PARENT root
#define main remountMain
#define getmntinfo fixtureMountInventory
#define NetFSMountURLSync fixtureNetFSMount
#define fsync fixtureFsync
#define time fixtureTime
#include "macos-nas-remount.m"
#undef time
#undef fsync
#undef main

#include <assert.h>

static struct statfs inventory[2];
static int entries;
static int inventoryCalls;
static int mountCalls;
static int mountStatus;
static BOOL publishMount;
static BOOL holdPresentAtCall;
static int syncCalls;
static int failSyncAt;
static time_t now;
static char stateIdentity[64];
static const char *expectedURL = "smb://operator@nas.example.test/disposable";
static const char *expectedSource = "//operator@nas.example.test/disposable";
static const char *mountedSource;
static const char *mountedPath;
static const char *reportedPath;
static NSString *point;
static NSString *stateDirectory;
static NSString *hold;
static NSString *localFile;
static NSString *movedPoint;
static const char *args[6];
static NSString *refusal;
static NSString *retryState;
static void (*beforeMount)(void);
static void (*duringMount)(void);

static int runRemount(void) {
    return remountMain(6, args);
}

static int runCheck(void) {
    const char *check[] = {args[0], "--check", args[1], args[2], args[3], args[4], args[5]};
    return remountMain(7, check);
}

time_t fixtureTime(time_t *result) {
    assert(result == NULL);
    return now;
}

static void setEntry(int index, const char *type, const char *path, const char *source) {
    strlcpy(inventory[index].f_fstypename, type, sizeof(inventory[index].f_fstypename));
    strlcpy(inventory[index].f_mntonname, path, sizeof(inventory[index].f_mntonname));
    strlcpy(inventory[index].f_mntfromname, source, sizeof(inventory[index].f_mntfromname));
}

static void mutateTargetBeforeMount(void) {
    if (beforeMount != NULL) {
        beforeMount();
        beforeMount = NULL;
    }
}

int fixtureMountInventory(struct statfs **mounts, int flags) {
    assert(flags == MNT_NOWAIT);
    if (++inventoryCalls == 2) {
        mutateTargetBeforeMount();
    }
    *mounts = inventory;
    return entries;
}

static int fixtureFsync(int fd) {
    if (++syncCalls == failSyncAt) {
        errno = EIO;
        return -1;
    }
    return fsync(fd);
}

int fixtureNetFSMount(CFURLRef url, CFURLRef mountpath, CFStringRef user, CFStringRef password,
                      CFMutableDictionaryRef session, CFMutableDictionaryRef options,
                      CFArrayRef *mounted) {
    assert([(__bridge NSString *)CFURLGetString(url) isEqualToString:@(args[1])]);
    assert(user == NULL && password == NULL);
    assert(CFEqual(CFDictionaryGetValue(session, kNAUIOptionKey), kNAUIOptionNoUI));
    struct stat target;
    if (lstat(args[2], &target) == 0) {
        assert(mountpath != NULL);
        assert(strcmp([(__bridge NSURL *)mountpath fileSystemRepresentation], args[2]) == 0);
        assert(CFEqual(CFDictionaryGetValue(options, kNetFSMountAtMountDirKey), kCFBooleanTrue));
    } else {
        assert(errno == ENOENT && mountpath == NULL);
        assert(CFDictionaryGetValue(options, kNetFSMountAtMountDirKey) == NULL);
    }
    mountCalls++;
    struct stat state;
    holdPresentAtCall = lstat(hold.fileSystemRepresentation, &state) == 0;
    mutateTargetBeforeMount();
    if (duringMount != NULL) {
        duringMount();
    }
    *mounted = NULL;
    if (mountStatus == 0 && publishMount) {
        const char *path = mountedPath ?: args[2];
        if (mountpath == NULL) {
            assert(mkdir(path, 0700) == 0);
        }
        setEntry(1, "smbfs", path, mountedSource ?: expectedSource);
        *mounted = CFBridgingRetain(@[@(reportedPath ?: path)]);
        entries = 2;
    }
    return mountStatus;
}

static void resetFixture(void) {
    char temporary[PATH_MAX];
    const char *temporaryDirectory = getenv("TMPDIR");
    NSString *parent = temporaryDirectory == NULL ? NSTemporaryDirectory() : @(temporaryDirectory);
    NSString *pattern = [parent stringByAppendingPathComponent:@"myce-remount-test-XXXXXX"];
    assert(strlcpy(temporary, pattern.fileSystemRepresentation, sizeof(temporary)) < sizeof(temporary));
    char canonical[PATH_MAX];
    assert(mkdtemp(temporary) != NULL && realpath(temporary, canonical) != NULL);
    root = @(canonical);
    point = [root stringByAppendingPathComponent:@"target"];
    movedPoint = [root stringByAppendingPathComponent:@"moved-target"];
    localFile = [point stringByAppendingPathComponent:@"local-data"];
    stateDirectory = [root stringByAppendingPathComponent:@"state"];
    hold = [stateDirectory stringByAppendingPathComponent:@"auth-hold"];
    refusal = [stateDirectory stringByAppendingPathComponent:@"refused-mount"];
    retryState = [stateDirectory stringByAppendingPathComponent:@"network-retry"];
    assert(mkdir(point.fileSystemRepresentation, 0700) == 0);
    assert(mkdir(stateDirectory.fileSystemRepresentation, 0700) == 0);
    struct stat state;
    assert(stat(stateDirectory.fileSystemRepresentation, &state) == 0);
    int length = snprintf(stateIdentity, sizeof(stateIdentity), "%u:%llu",
                          (unsigned int)state.st_dev, (unsigned long long)state.st_ino);
    assert(length > 0 && (size_t)length < sizeof(stateIdentity));
    const char *remountArgs[] = {"remount", expectedURL, point.fileSystemRepresentation,
                                 expectedSource, stateDirectory.fileSystemRepresentation, stateIdentity};
    memcpy(args, remountArgs, sizeof(args));
    entries = 1;
    setEntry(0, "apfs", "/", "/dev/fixture");
    inventoryCalls = mountCalls = mountStatus = 0;
    publishMount = holdPresentAtCall = NO;
    syncCalls = failSyncAt = 0;
    now = 10000;
    mountedSource = mountedPath = reportedPath = NULL;
    beforeMount = duringMount = NULL;
}

static void clearHold(void) {
    assert(unlink(hold.fileSystemRepresentation) == 0);
}

static void assertNoHold(void) {
    struct stat state;
    assert(lstat(hold.fileSystemRepresentation, &state) == -1 && errno == ENOENT);
}

static void cleanFixture(void) {
    assert(unlink(hold.fileSystemRepresentation) == 0 || errno == ENOENT);
    assert(unlink(refusal.fileSystemRepresentation) == 0 || errno == ENOENT);
    assert(unlink(retryState.fileSystemRepresentation) == 0 || errno == ENOENT);
    assert(rmdir(stateDirectory.fileSystemRepresentation) == 0 || errno == ENOENT);
    assert(rmdir(point.fileSystemRepresentation) == 0 || errno == ENOENT);
    assert(rmdir(root.fileSystemRepresentation) == 0);
}

static void addLocalFile(void) {
    int fd = open(localFile.fileSystemRepresentation, O_WRONLY | O_CREAT | O_EXCL, 0600);
    assert(fd >= 0 && close(fd) == 0);
}

static void replaceTarget(void) {
    assert(rename(point.fileSystemRepresentation, movedPoint.fileSystemRepresentation) == 0);
    assert(mkdir(point.fileSystemRepresentation, 0700) == 0);
}

static void restoreTarget(void) {
    assert(rmdir(point.fileSystemRepresentation) == 0);
    assert(rename(movedPoint.fileSystemRepresentation, point.fileSystemRepresentation) == 0);
}

static void createTarget(void) {
    assert(mkdir(point.fileSystemRepresentation, 0700) == 0);
}

static void inventoryAndCredentialOptions(void) {
    entries = 0;
    assert(runRemount() == EX_OSERR);
    entries = 1;
    assert(runCheck() == EX_UNAVAILABLE);

    entries = 2;
    setEntry(1, "smbfs", args[2], "//operator@nas.example.test/other");
    assert(runRemount() == EX_CONFIG);
    setEntry(1, "smbfs", "/elsewhere", expectedSource);
    assert(runRemount() == EX_CONFIG);
    setEntry(1, "smbfs", args[2], expectedSource);
    assert(runRemount() == EX_OK);
    assert(mountCalls == 0);

    entries = 1;
    addLocalFile();
    assert(runRemount() == EX_CONFIG);
    assert(unlink(localFile.fileSystemRepresentation) == 0);
    args[1] = "smb://operator:fixture-password@nas.example.test/disposable";
    assert(runRemount() == EX_USAGE);
    assert(mountCalls == 0);
    args[1] = expectedURL;

    publishMount = YES;
    assert(runRemount() == EX_OK);
    assert(runCheck() == EX_OK);
    assert(mountCalls == 1);
    assertNoHold();
}

static void rejectsMismatchedSourceURL(void) {
    publishMount = YES;
    const char *differentURLs[] = {"smb://operator@nas.example.test/other",
                                   "smb://operator@other.example.test/disposable",
                                   "smb://other@nas.example.test/disposable",
                                   "smb://operator@nas.example.test:1445/disposable"};
    for (size_t i = 0; i < sizeof(differentURLs) / sizeof(differentURLs[0]); i++) {
        args[1] = differentURLs[i];
        mountedSource = differentURLs[i] + strlen("smb:");
        assert(runRemount() == EX_CONFIG);
        assert(runCheck() == EX_CONFIG);
        assert(mountCalls == 0);
    }

    args[1] = "smb://operator@nas.example.test/escaped%20share";
    args[3] = mountedSource = "//operator@nas.example.test/escaped%20share";
    assert(runRemount() == EX_OK);
    assert(mountCalls == 1);
}

static void missingStatePreventsAuthentication(void) {
    assert(rmdir(stateDirectory.fileSystemRepresentation) == 0);
    assert(runRemount() == 80);
    assert(runCheck() == 80);
    assert(mountCalls == 0);
}

static void assertSyncFailurePreventsAuthentication(int failedSync) {
    publishMount = YES;
    failSyncAt = failedSync;
    assert(runRemount() == EX_CANTCREAT);
    assert(mountCalls == 0);
    assertNoHold();
    assert(runRemount() == EX_OK);
    assert(holdPresentAtCall && mountCalls == 1);
}

static void firstSyncFailurePreventsAuthentication(void) {
    assertSyncFailurePreventsAuthentication(1);
}

static void directorySyncFailurePreventsAuthentication(void) {
    assertSyncFailurePreventsAuthentication(2);
}

static void retriesDisconnectsWithinBudget(void) {
    mountStatus = ENOTCONN;
    assert(runRemount() == EX_TEMPFAIL);
    assert(holdPresentAtCall && mountCalls == 1);
    assertNoHold();
    now += 119;
    assert(runRemount() == EX_TEMPFAIL && mountCalls == 1);

    now++;
    mountStatus = EPIPE;
    assert(runRemount() == EX_TEMPFAIL && mountCalls == 2);
    assertNoHold();
    now += 120;
    assert(runRemount() == EX_TEMPFAIL && mountCalls == 2);
    now += 659;
    assert(runRemount() == EX_TEMPFAIL && mountCalls == 2);
    now++;
    mountStatus = 0;
    publishMount = YES;
    assert(runRemount() == EX_OK && mountCalls == 3);
    assertNoHold();
}

static void retriesUncertainTimeoutWithinBudget(void) {
    mountStatus = ETIMEDOUT;
    assert(runRemount() == EX_TEMPFAIL);
    assert(holdPresentAtCall && mountCalls == 1);
    assertNoHold();
    assert(runCheck() == EX_UNAVAILABLE);
    now += 119;
    assert(runRemount() == EX_TEMPFAIL && mountCalls == 1);
    now++;
    assert(runRemount() == EX_TEMPFAIL && mountCalls == 2);
}

static void retriesDisconnectClassesWithinBudget(void) {
    const int disconnects[] = {ENETRESET, ECONNABORTED};
    for (size_t i = 0; i < sizeof(disconnects) / sizeof(disconnects[0]); i++) {
        mountStatus = disconnects[i];
        assert(runRemount() == EX_TEMPFAIL);
        assert(mountCalls == (int)i + 1);
        assertNoHold();
        now += 120;
    }
}

static void holdsNonTransientResults(void) {
    const int nonTransient[] = {EAUTH, EIO};
    for (size_t i = 0; i < sizeof(nonTransient) / sizeof(nonTransient[0]); i++) {
        mountStatus = nonTransient[i];
        assert(runRemount() == EX_NOPERM);
        assert(holdPresentAtCall);
        assert(runRemount() == EX_NOPERM);
        assert(mountCalls == (int)i + 1);
        clearHold();
    }
}

static void holdsUnsupportedAuthentication(void) {
    mountStatus = ENETFSNOAUTHMECHSUPP;
    assert(runRemount() == EX_NOPERM);
    assert(holdPresentAtCall);
    assert(runRemount() == EX_NOPERM);
    assert(mountCalls == 1);
}

static void moveAttemptState(void) {
    assert(rename(stateDirectory.fileSystemRepresentation, movedPoint.fileSystemRepresentation) == 0);
    assert(mkdir(stateDirectory.fileSystemRepresentation, 0700) == 0);
}

static void restoreAttemptState(void) {
    assert(rmdir(stateDirectory.fileSystemRepresentation) == 0);
    assert(rename(movedPoint.fileSystemRepresentation, stateDirectory.fileSystemRepresentation) == 0);
}

static void rejectsReplacedAttemptStateBeforeNetFS(void) {
    beforeMount = moveAttemptState;
    assert(runRemount() == 80);
    assert(mountCalls == 0);
    assert(runCheck() == 80);
    restoreAttemptState();
    assertNoHold();
}

static void rejectsReplacedAttemptStateAfterNetFS(void) {
    mountStatus = EAUTH;
    duringMount = moveAttemptState;
    int first = runRemount();
    duringMount = NULL;
    int following = runRemount();
    assert(mountCalls == 1);
    assert(first == 80 && following == 80);
    entries = 2;
    setEntry(1, "smbfs", args[2], expectedSource);
    assert(runCheck() == 80);
    restoreAttemptState();
    assert(runCheck() == EX_NOPERM);
    clearHold();
}

static void rejectsAncestorSymlink(void) {
    NSString *alias = [root stringByAppendingPathComponent:@"alias"];
    assert(symlink(root.fileSystemRepresentation, alias.fileSystemRepresentation) == 0);
    NSString *aliasedPoint = [alias stringByAppendingPathComponent:@"target"];
    args[2] = aliasedPoint.fileSystemRepresentation;
    assert(runRemount() == EX_CONFIG);
    assert(rmdir(point.fileSystemRepresentation) == 0);
    assert(runRemount() == EX_CONFIG);
    assert(mountCalls == 0);
    assert(unlink(alias.fileSystemRepresentation) == 0);
}

static void retriesAfterTargetChangesBeforeNetFS(void) {
    publishMount = YES;
    beforeMount = addLocalFile;
    assert(runRemount() == EX_CONFIG);
    assert(mountCalls == 0);
    assert(unlink(localFile.fileSystemRepresentation) == 0);
    assert(runRemount() == EX_OK && mountCalls == 1);
    assertNoHold();

    entries = 1;
    inventoryCalls = 0;
    beforeMount = replaceTarget;
    assert(runRemount() == EX_CONFIG && mountCalls == 1);
    restoreTarget();
    assertNoHold();

    inventoryCalls = 0;
    assert(rmdir(point.fileSystemRepresentation) == 0);
    beforeMount = createTarget;
    assert(runRemount() == EX_CONFIG && mountCalls == 1);
    assertNoHold();
    assert(runRemount() == EX_OK && mountCalls == 2);
}

static void loseInventory(void) {
    entries = 0;
}

static void retriesAfterSecondInventoryReadFails(void) {
    beforeMount = loseInventory;
    assert(runRemount() == EX_OSERR);
    assert(mountCalls == 0);
    entries = 1;
    publishMount = YES;
    assert(runRemount() == EX_OK && mountCalls == 1);
    assertNoHold();
}

static void assertPersistentRefusal(void) {
    int calls = mountCalls;
    assert(runCheck() == 79);
    assert(runRemount() == 79 && mountCalls == calls);
    assertNoHold();
    assert(unlink(refusal.fileSystemRepresentation) == 0);
}

static void rejectsChangesDuringNetFSAndUnexpectedMount(void) {
    publishMount = YES;
    duringMount = addLocalFile;
    assert(runRemount() == 79);
    assert(unlink(localFile.fileSystemRepresentation) == 0);
    assertPersistentRefusal(); // The exact mount is visible but was refused.
    assert(runCheck() == EX_OK);

    entries = 1;
    duringMount = replaceTarget;
    assert(runRemount() == 79);
    restoreTarget();
    assertPersistentRefusal();

    entries = 1;
    duringMount = NULL;
    mountedSource = "//operator@nas.example.test/other";
    assert(runRemount() == 79);
    assertPersistentRefusal();

    entries = 1;
    mountedSource = NULL;
    publishMount = NO;
    assert(runRemount() == 79); // NetFS OK alone proves nothing.
    assertPersistentRefusal();
    assert(mountCalls == 4);
}

static void recreatesTargetRemovedByUnmount(void) {
    entries = 2;
    setEntry(1, "smbfs", args[2], expectedSource);
    assert(runRemount() == EX_OK && mountCalls == 0);

    entries = 1;
    assert(rmdir(point.fileSystemRepresentation) == 0);
    assert(runCheck() == EX_UNAVAILABLE && mountCalls == 0);
    publishMount = YES;
    assert(runRemount() == EX_OK && mountCalls == 1);
    assert(holdPresentAtCall);
    assert(runCheck() == EX_OK);
    assertNoHold();
}

static void refusesAlternateDefaultMount(void) {
    assert(rmdir(point.fileSystemRepresentation) == 0);
    NSString *alternate = [point stringByAppendingString:@"-1"];
    mountedPath = alternate.fileSystemRepresentation;
    publishMount = YES;
    assert(runRemount() == 79 && mountCalls == 1);
    assert(runCheck() == 79 && runRemount() == 79 && mountCalls == 1);
    // The unexpected mount is still present at the path selected by NetFS.
    assert(checkMount(mountedPath, expectedSource) == EX_OK);
    assert(rmdir(mountedPath) == 0);
    assertNoHold();
}

static void refusesMismatchedNetFSPaths(void) {
    assert(rmdir(point.fileSystemRepresentation) == 0);
    reportedPath = movedPoint.fileSystemRepresentation;
    publishMount = YES;
    assert(runRemount() == 79 && mountCalls == 1);
    assert(checkMount(args[2], expectedSource) == EX_OK);
    assertPersistentRefusal();
}

static void refusesOccupiedTargetBeforeNetFS(void) {
    assert(rmdir(point.fileSystemRepresentation) == 0);
    int fd = open(point.fileSystemRepresentation, O_WRONLY | O_CREAT | O_EXCL, 0600);
    assert(fd >= 0 && close(fd) == 0);
    assert(runCheck() == EX_CONFIG);
    assert(runRemount() == EX_CONFIG);
    assert(unlink(point.fileSystemRepresentation) == 0);

    assert(symlink(movedPoint.fileSystemRepresentation, point.fileSystemRepresentation) == 0);
    assert(runCheck() == EX_CONFIG);
    assert(runRemount() == EX_CONFIG);
    assert(unlink(point.fileSystemRepresentation) == 0);

    assert(mkdir(point.fileSystemRepresentation, 0700) == 0);
    addLocalFile();
    assert(runCheck() == EX_CONFIG);
    assert(runRemount() == EX_CONFIG);
    assert(unlink(localFile.fileSystemRepresentation) == 0);
    assert(runCheck() == EX_UNAVAILABLE);
    assert(mountCalls == 0);
    assertNoHold();
}

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        const struct { const char *name; void (*test)(void); } fixtures[] = {
            {"inventoryAndCredentialOptions", inventoryAndCredentialOptions},
            {"rejectsMismatchedSourceURL", rejectsMismatchedSourceURL},
            {"missingStatePreventsAuthentication", missingStatePreventsAuthentication},
            {"firstSyncFailurePreventsAuthentication", firstSyncFailurePreventsAuthentication},
            {"directorySyncFailurePreventsAuthentication", directorySyncFailurePreventsAuthentication},
            {"retriesDisconnectsWithinBudget", retriesDisconnectsWithinBudget},
            {"retriesUncertainTimeoutWithinBudget", retriesUncertainTimeoutWithinBudget},
            {"retriesDisconnectClassesWithinBudget", retriesDisconnectClassesWithinBudget},
            {"holdsNonTransientResults", holdsNonTransientResults},
            {"holdsUnsupportedAuthentication", holdsUnsupportedAuthentication},
            {"rejectsReplacedAttemptStateBeforeNetFS", rejectsReplacedAttemptStateBeforeNetFS},
            {"rejectsReplacedAttemptStateAfterNetFS", rejectsReplacedAttemptStateAfterNetFS},
            {"rejectsAncestorSymlink", rejectsAncestorSymlink},
            {"retriesAfterTargetChangesBeforeNetFS", retriesAfterTargetChangesBeforeNetFS},
            {"retriesAfterSecondInventoryReadFails", retriesAfterSecondInventoryReadFails},
            {"rejectsChangesDuringNetFSAndUnexpectedMount", rejectsChangesDuringNetFSAndUnexpectedMount},
            {"recreatesTargetRemovedByUnmount", recreatesTargetRemovedByUnmount},
            {"refusesAlternateDefaultMount", refusesAlternateDefaultMount},
            {"refusesMismatchedNetFSPaths", refusesMismatchedNetFSPaths},
            {"refusesOccupiedTargetBeforeNetFS", refusesOccupiedTargetBeforeNetFS},
        };
        for (size_t i = 0; i < sizeof(fixtures) / sizeof(fixtures[0]); i++) {
            if (argc == 1 || (argc == 2 && strcmp(fixtures[i].name, argv[1]) == 0)) {
                resetFixture();
                fixtures[i].test();
                cleanFixture();
                if (argc == 2) {
                    return 0;
                }
            }
        }
        return argc == 1 ? 0 : EX_USAGE;
    }
}
