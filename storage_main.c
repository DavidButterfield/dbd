char * const name = "tcmu";
char * const arg1 = "-d";
char *argv[3] = { name, arg1, 0 };
extern int tcmu_main(int, char **);
int tcmu_main_thunk(void)
{
    return tcmu_main(2, argv);
}
