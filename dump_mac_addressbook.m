#include <stdio.h>
#include <AddressBook/AddressBook.h>

static void print_propstr(const char *str)
{
    printf("%s\n", str ? str : "");
}

int main(int argc, char **argv)
{
    @autoreleasepool {
        int idx = 0;

        NSArray *list = [[ABAddressBook sharedAddressBook] people];

        for (ABPerson *person in list) {
            print_propstr([[person valueForProperty:kABNicknameProperty] cStringUsingEncoding:NSUTF8StringEncoding]);
            print_propstr([[person valueForProperty:kABFirstNameProperty] cStringUsingEncoding:NSUTF8StringEncoding]);
            print_propstr([[person valueForProperty:kABMiddleNameProperty] cStringUsingEncoding:NSUTF8StringEncoding]);
            print_propstr([[person valueForProperty:kABLastNameProperty] cStringUsingEncoding:NSUTF8StringEncoding]);
            static NSString *props[] = { @"Phone", @"Email", nil };
            for (int propidx = 0; props[propidx] != nil; propidx++) {
                ABMultiValue *mval = [person valueForProperty:props[propidx]];
                const int total = [mval count];
                for (int i = 0; i < total; i++) {
                    NSString *nsstr = [mval valueAtIndex:i];
                    printf("%s\n", [nsstr cStringUsingEncoding:NSUTF8StringEncoding]);
                }
                printf("\n");
            }
        }
    }

    return 0;
}

// end of dump_mac_addressbook.m ...
