#include <stdio.h>

int run_callback(void);
int run_pull_buffer(void);
int run_push_buffer(void);

int main(void) {
    int result = 0;
    printf("Callback solution:\n");
    int r = run_callback();
    if (r != 0) {
        result = r;
    }

    printf("\nPull buffer solution:\n");
    r = run_pull_buffer();
    if (r != 0) {
        result = r;
    }

    printf("\nPush buffer solution:\n");
    r = run_push_buffer();
    if (r != 0) {
        result = r;
    }

    return result;
}
