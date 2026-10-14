//go:build darwin && cgo

#import <Foundation/Foundation.h>
#import <CoreGraphics/CoreGraphics.h>
#import <IOKit/ps/IOPowerSources.h>
#import <IOKit/ps/IOPSKeys.h>
#include <math.h>

void myce_owner_signals(double *idle, int *power, int *laptop, int *heat) {
 @autoreleasepool {
  // Query aggregate event age. No event tap, input capture or special access.
  *idle = CGEventSourceSecondsSinceLastEventType(kCGEventSourceStateCombinedSessionState, kCGAnyInputEventType);
  if (!isfinite(*idle) || *idle < 0) *idle = -1;
  *power = -1; *laptop = 0; *heat = -1;
  CFTypeRef info = IOPSCopyPowerSourcesInfo();
  if (info) {
   CFArrayRef sources = IOPSCopyPowerSourcesList(info);
   if (sources) {
    for (CFIndex i = 0; i < CFArrayGetCount(sources); i++) {
     CFDictionaryRef source = IOPSGetPowerSourceDescription(info, CFArrayGetValueAtIndex(sources, i));
     if (source && CFEqual(CFDictionaryGetValue(source, CFSTR(kIOPSTypeKey)) ?: CFSTR(""), CFSTR(kIOPSInternalBatteryType))) *laptop = 1;
    }
    CFStringRef providing = IOPSGetProvidingPowerSourceType(info);
    if (providing && CFEqual(providing, CFSTR(kIOPSACPowerValue))) *power = *laptop ? 1 : 0;
    else if (providing && (CFEqual(providing, CFSTR(kIOPSBatteryPowerValue)) || CFEqual(providing, CFSTR(kIOPMUPSPowerKey)))) *power = 2;
    CFRelease(sources);
   }
   CFRelease(info);
  }
  NSProcessInfo *process = [NSProcessInfo processInfo];
  if ([process respondsToSelector:@selector(thermalState)]) *heat = (int)process.thermalState;
 }
}
