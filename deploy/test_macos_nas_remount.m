// Replace OS calls with fixtures. No SMB or mount syscall runs in these tests.
#include <unistd.h>
static int fixtureFsync(int fd);
#define main remountMain
#define getmntinfo fixtureMountInventory
#define NetFSMountURLSync fixtureNetFSMount
#define fsync fixtureFsync
#include "macos-nas-remount.m"
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
static BOOL failSync;
static const char *expectedURL = "smb://operator@nas.example.test/disposable";
static const char *expectedSource = "//operator@nas.example.test/disposable";
static const char *mountedSource;
static const char *mountedPath;
static NSString *root;
static NSString *point;
static NSString *stateDirectory;
static NSString *hold;
static NSString *localFile;
static NSString *movedPoint;
static const char *args[5];
static const char *check[5];
static void (*beforeMount)(void);
static void (*duringMount)(void);

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
    if (failSync) {
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
    assert(CFEqual(CFDictionaryGetValue(options, kNetFSMountAtMountDirKey), kCFBooleanTrue));
    mountCalls++;
    struct stat state;
    holdPresentAtCall = lstat(hold.fileSystemRepresentation, &state) == 0;
    mutateTargetBeforeMount();
    if (duringMount != NULL) {
        duringMount();
    }
    *mounted = NULL;
    if (mountStatus == 0 && publishMount) {
        setEntry(1, "smbfs", mountedPath ?: [(__bridge NSURL *)mountpath fileSystemRepresentation],
                 mountedSource ?: expectedSource);
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
    assert(mkdir(point.fileSystemRepresentation, 0700) == 0);
    assert(mkdir(stateDirectory.fileSystemRepresentation, 0700) == 0);
    const char *remountArgs[] = {"remount", expectedURL, point.fileSystemRepresentation,
                                 expectedSource, hold.fileSystemRepresentation};
    const char *checkArgs[] = {"remount", "--check", remountArgs[1], remountArgs[2], remountArgs[3]};
    memcpy(args, remountArgs, sizeof(args));
    memcpy(check, checkArgs, sizeof(check));
    entries = 1;
    setEntry(0, "apfs", "/", "/dev/fixture");
    inventoryCalls = mountCalls = mountStatus = 0;
    publishMount = holdPresentAtCall = failSync = NO;
    mountedSource = mountedPath = NULL;
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

static void inventoryAndCredentialOptions(void) {
    entries = 0;
    assert(remountMain(5, args) == EX_OSERR);
    entries = 1;
    assert(remountMain(5, check) == EX_UNAVAILABLE);

    entries = 2;
    setEntry(1, "smbfs", args[2], "//operator@nas.example.test/other");
    assert(remountMain(5, args) == EX_CONFIG);
    setEntry(1, "smbfs", "/elsewhere", expectedSource);
    assert(remountMain(5, args) == EX_CONFIG);
    setEntry(1, "smbfs", args[2], expectedSource);
    assert(remountMain(5, args) == EX_OK);
    assert(mountCalls == 0);

    entries = 1;
    addLocalFile();
    assert(remountMain(5, args) == EX_CONFIG);
    assert(unlink(localFile.fileSystemRepresentation) == 0);
    args[1] = "smb://operator:fixture-password@nas.example.test/disposable";
    assert(remountMain(5, args) == EX_USAGE);
    assert(mountCalls == 0);
    args[1] = expectedURL;

    publishMount = YES;
    assert(remountMain(5, args) == EX_OK);
    assert(remountMain(5, check) == EX_OK);
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
        check[2] = args[1] = differentURLs[i];
        mountedSource = differentURLs[i] + strlen("smb:");
        assert(remountMain(5, args) == EX_CONFIG);
        assert(remountMain(5, check) == EX_CONFIG);
        assert(mountCalls == 0);
    }

    args[1] = "smb://operator@nas.example.test/escaped%20share";
    args[3] = mountedSource = "//operator@nas.example.test/escaped%20share";
    assert(remountMain(5, args) == EX_OK);
    assert(mountCalls == 1);
}

static void missingOrUnsyncedStatePreventsAuthentication(void) {
    mountStatus = EAUTH;
    assert(rmdir(stateDirectory.fileSystemRepresentation) == 0);
    assert(remountMain(5, args) == EX_CANTCREAT);
    assert(remountMain(5, args) == EX_CANTCREAT);
    assert(mountCalls == 0);

    assert(mkdir(stateDirectory.fileSystemRepresentation, 0700) == 0);
    failSync = YES;
    assert(remountMain(5, args) == EX_CANTCREAT);
    failSync = NO;
    assert(remountMain(5, args) == EX_NOPERM);
    assert(mountCalls == 0);
}

static void holdsNonTransientResultsAndRetriesNetworkTimeout(void) {
    mountStatus = ETIMEDOUT;
    assert(remountMain(5, args) == EX_TEMPFAIL);
    assert(holdPresentAtCall);
    assertNoHold();
    const int nonTransient[] = {EAUTH, EIO};
    for (size_t i = 0; i < sizeof(nonTransient) / sizeof(nonTransient[0]); i++) {
        mountStatus = nonTransient[i];
        assert(remountMain(5, args) == EX_NOPERM);
        assert(holdPresentAtCall);
        assert(remountMain(5, args) == EX_NOPERM);
        assert(mountCalls == (int)i + 2);
        clearHold();
    }
}

static void holdsUnsupportedAuthentication(void) {
    mountStatus = ENETFSNOAUTHMECHSUPP;
    assert(remountMain(5, args) == EX_NOPERM);
    assert(holdPresentAtCall);
    assert(remountMain(5, args) == EX_NOPERM);
    assert(mountCalls == 1);
}

static void moveAttemptState(void) {
    assert(rename(stateDirectory.fileSystemRepresentation, movedPoint.fileSystemRepresentation) == 0);
    assert(mkdir(stateDirectory.fileSystemRepresentation, 0700) == 0);
}

static void rejectsReplacedAttemptStateBeforeNetFS(void) {
    beforeMount = moveAttemptState;
    assert(remountMain(5, args) == EX_CANTCREAT);
    assert(mountCalls == 0);
    assert(rmdir(stateDirectory.fileSystemRepresentation) == 0);
    assert(rename(movedPoint.fileSystemRepresentation, stateDirectory.fileSystemRepresentation) == 0);
    clearHold();
}

static void rejectsAncestorSymlink(void) {
    NSString *alias = [root stringByAppendingPathComponent:@"alias"];
    assert(symlink(root.fileSystemRepresentation, alias.fileSystemRepresentation) == 0);
    NSString *aliasedPoint = [alias stringByAppendingPathComponent:@"target"];
    args[2] = aliasedPoint.fileSystemRepresentation;
    assert(remountMain(5, args) == EX_CONFIG);
    assert(mountCalls == 0);
    assert(unlink(alias.fileSystemRepresentation) == 0);
}

static void rejectsTargetChangesBeforeNetFS(void) {
    publishMount = YES;
    beforeMount = addLocalFile;
    assert(remountMain(5, args) == EX_CONFIG);
    assert(mountCalls == 0);
    assert(unlink(localFile.fileSystemRepresentation) == 0);
    clearHold();

    inventoryCalls = 0;
    beforeMount = replaceTarget;
    assert(remountMain(5, args) == EX_CONFIG);
    assert(mountCalls == 0);
    restoreTarget();
}

static void rejectsChangesDuringNetFSAndUnexpectedMount(void) {
    publishMount = YES;
    duringMount = addLocalFile;
    assert(remountMain(5, args) == EX_CONFIG);
    assert(unlink(localFile.fileSystemRepresentation) == 0);
    clearHold();

    entries = 1;
    duringMount = replaceTarget;
    assert(remountMain(5, args) == EX_CONFIG);
    restoreTarget();
    clearHold();

    entries = 1;
    duringMount = NULL;
    mountedSource = "//operator@nas.example.test/other";
    assert(remountMain(5, args) == EX_CONFIG);
    clearHold();

    entries = 1;
    mountedSource = NULL;
    mountedPath = "/elsewhere";
    assert(remountMain(5, args) == EX_CONFIG);
    clearHold();

    entries = 1;
    publishMount = NO;
    assert(remountMain(5, args) == EX_CONFIG); // NetFS OK alone proves nothing.
    assert(remountMain(5, args) == EX_NOPERM);
    assert(mountCalls == 5);
}

static void installCheckRequiresSurvivingEmptyTarget(void) {
    assert(rmdir(point.fileSystemRepresentation) == 0);
    assert(remountMain(5, check) == EX_CONFIG);
    assert(mkdir(point.fileSystemRepresentation, 0700) == 0);
    addLocalFile();
    assert(remountMain(5, check) == EX_CONFIG);
    assert(unlink(localFile.fileSystemRepresentation) == 0);
    assert(remountMain(5, check) == EX_UNAVAILABLE);
    assert(mountCalls == 0);
    assertNoHold();
}

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        const struct { const char *name; void (*test)(void); } fixtures[] = {
            {"inventoryAndCredentialOptions", inventoryAndCredentialOptions},
            {"rejectsMismatchedSourceURL", rejectsMismatchedSourceURL},
            {"missingOrUnsyncedStatePreventsAuthentication", missingOrUnsyncedStatePreventsAuthentication},
            {"holdsNonTransientResultsAndRetriesNetworkTimeout", holdsNonTransientResultsAndRetriesNetworkTimeout},
            {"holdsUnsupportedAuthentication", holdsUnsupportedAuthentication},
            {"rejectsReplacedAttemptStateBeforeNetFS", rejectsReplacedAttemptStateBeforeNetFS},
            {"rejectsAncestorSymlink", rejectsAncestorSymlink},
            {"rejectsTargetChangesBeforeNetFS", rejectsTargetChangesBeforeNetFS},
            {"rejectsChangesDuringNetFSAndUnexpectedMount", rejectsChangesDuringNetFSAndUnexpectedMount},
            {"installCheckRequiresSurvivingEmptyTarget", installCheckRequiresSurvivingEmptyTarget},
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
