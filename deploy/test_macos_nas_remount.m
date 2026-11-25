// Replace OS calls with fixtures. No SMB or mount syscall runs in these tests.
#define main remountMain
#define getmntinfo fixtureMountInventory
#define NetFSMountURLSync fixtureNetFSMount
#include "macos-nas-remount.m"
#undef main

#include <assert.h>

static struct statfs inventory[2];
static int entries;
static int mountCalls;
static int mountStatus;
static BOOL publishMount;
static const char *expectedSource = "//operator@nas.example.test/disposable";

static void setEntry(int index, const char *type, const char *path, const char *source) {
    strlcpy(inventory[index].f_fstypename, type, sizeof(inventory[index].f_fstypename));
    strlcpy(inventory[index].f_mntonname, path, sizeof(inventory[index].f_mntonname));
    strlcpy(inventory[index].f_mntfromname, source, sizeof(inventory[index].f_mntfromname));
}

int fixtureMountInventory(struct statfs **mounts, int flags) {
    assert(flags == MNT_NOWAIT);
    *mounts = inventory;
    return entries;
}

int fixtureNetFSMount(CFURLRef url, CFURLRef mountpath, CFStringRef user, CFStringRef password,
                      CFMutableDictionaryRef session, CFMutableDictionaryRef options,
                      CFArrayRef *mounted) {
    assert(CFEqual(CFURLGetString(url), CFSTR("smb://operator@nas.example.test/disposable")));
    assert(user == NULL && password == NULL);
    assert(CFEqual(CFDictionaryGetValue(session, kNAUIOptionKey), kNAUIOptionNoUI));
    assert(CFEqual(CFDictionaryGetValue(options, kNetFSMountAtMountDirKey), kCFBooleanTrue));
    mountCalls++;
    *mounted = NULL;
    if (mountStatus == 0 && publishMount) {
        setEntry(1, "smbfs", [(__bridge NSURL *)mountpath fileSystemRepresentation], expectedSource);
        entries = 2;
    }
    return mountStatus;
}

int main(void) {
    @autoreleasepool {
        char directory[] = "/tmp/myce-remount-test-XXXXXX";
        assert(mkdtemp(directory) != NULL);
        NSString *point = @(directory).stringByStandardizingPath;
        NSString *hold = [point stringByAppendingString:@".auth-hold"];
        const char *args[] = {"remount", "smb://operator@nas.example.test/disposable",
                              point.fileSystemRepresentation, expectedSource,
                              hold.fileSystemRepresentation};
        const char *check[] = {"remount", "--check", args[1], args[2], args[3]};

        assert(remountMain(5, args) == EX_OSERR);
        entries = 1;
        setEntry(0, "apfs", "/", "/dev/fixture");
        assert(remountMain(5, check) == EX_UNAVAILABLE);
        assert(mountCalls == 0);

        entries = 2;
        setEntry(1, "smbfs", args[2], "//operator@nas.example.test/other");
        assert(remountMain(5, args) == EX_CONFIG);
        setEntry(1, "smbfs", "/elsewhere", expectedSource);
        assert(remountMain(5, args) == EX_CONFIG);
        setEntry(1, "smbfs", args[2], expectedSource);
        assert(remountMain(5, args) == EX_OK);
        assert(mountCalls == 0);

        entries = 1;
        NSString *file = [point stringByAppendingPathComponent:@"local-data"];
        int fd = open(file.fileSystemRepresentation, O_WRONLY | O_CREAT, 0600);
        assert(fd >= 0);
        assert(close(fd) == 0);
        assert(remountMain(5, args) == EX_CONFIG);
        assert(unlink(file.fileSystemRepresentation) == 0);
        assert(mountCalls == 0);

        args[1] = "smb://operator:fixture-password@nas.example.test/disposable";
        assert(remountMain(5, args) == EX_USAGE);
        assert(mountCalls == 0);
        args[1] = check[2];

        mountStatus = ETIMEDOUT;
        assert(remountMain(5, args) == EX_TEMPFAIL);
        mountStatus = EAUTH;
        assert(remountMain(5, args) == EX_NOPERM);
        mountStatus = 0;
        assert(remountMain(5, args) == EX_NOPERM);
        assert(mountCalls == 2);
        assert(unlink(hold.fileSystemRepresentation) == 0);

        assert(remountMain(5, args) == EX_CONFIG); // NetFS OK alone proves nothing.
        publishMount = YES;
        assert(remountMain(5, args) == EX_OK);
        assert(remountMain(5, check) == EX_OK);
        assert(mountCalls == 4);
        assert(rmdir(directory) == 0);
    }
    return 0;
}
