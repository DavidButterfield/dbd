#include "tcmu-runner.h"
char * const name = "tcmu";
char * const arg1 = "-d";
char *argv[3] = { name, arg1, 0 };
int tcmu_main_thunk(void)
{
    return tcmu_main(1, argv);
}
