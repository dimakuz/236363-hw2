#include <stdio.h>
#include <string.h>
#include "wet.h"
#include <libpq-fe.h>

PGconn *conn;

void* addUser(const char* name)
{
    PGresult *res;
    char param[200];
    sprintf(param, "INSERT INTO users "
    				"VALUES ( "
    						"1 + (SELECT MAX(id) FROM "
    						"(SELECT * FROM USERNAME UNION SELECT -1,'dummy') "
    						"AS users ), "

    				  "'%s' );"

    		, name);
    PQexec(conn, param);
    res = PQexec(conn, "SELECT MAX(id) FROM users;");
    printf(ADD_USER, PQgetvalue(res,0,0));
    PQclear(res);
}

void* removeUser (const char* id)
{
    PGresult *res;
    char param[200];
    sprintf(param, "SELECT * FROM users WHERE id = %d", atoi(id));
    res = PQexec(conn, param);
    if (!PQntuples(res)) 
    {
        printf(ILL_PARAMS);
        PQclear(res);
    }
    else 
    {
        PQclear(res);
        sprintf(param, "DELETE FROM users WHERE id = %d", atoi(id));
        res = PQexec(conn, param);
        PQclear(res);
    }
    sprintf(param, "DELETE FROM photos WHERE user_id = %d", atoi(id));
    PQexec(conn, param);
    sprintf(param, "DELETE FROM tags WHERE user_id = %d", atoi(id));
    PQexec(conn, param);
}

void* addPhoto (const char*    user_id,
                         const char*    photo_id)
{
    PGresult *res;
    char param[200];
    sprintf(param, "SELECT * FROM users WHERE id = %d", atoi(user_id));
    res = PQexec(conn, param);
    if (!PQntuples(res)) 
    {
        printf(ILL_PARAMS);
        PQclear(res);
        return;
    }
    PQclear(res);
    sprintf(param, "SELECT * FROM photos WHERE user_id = %d AND id = %d;",
                                            atoi(user_id), atoi(photo_id));
    res = PQexec(conn, param);
    if (PQntuples(res)) 
    {
        printf(EXISTING_RECORD);
        PQclear(res);
        return;
    }
    PQclear(res);
    sprintf(param, "INSERT INTO photos VALUES(%d,%d);",
                                           atoi(photo_id), atoi(user_id));
    PQexec(conn, param);
}

void* tagPhoto (const char* user_id,
                         const char* photo_id,
                         const char* info)
{
    PGresult *res;
    char param[200];
    sprintf(param, "SELECT * FROM users WHERE id = %d", atoi(user_id));
    res = PQexec(conn, param);
    if (!PQntuples(res)) 
    {
        printf(ILL_PARAMS);
        PQclear(res);	
        return;
    }
    PQclear(res);
    sprintf(param, "SELECT * FROM tags WHERE photo_id = %d AND "
                                            "user_id = %d AND "
                                             "info = '%s';",
                             atoi(photo_id), atoi(user_id), info);
    res = PQexec(conn, param);
    if (PQntuples(res))
    {
        printf(EXISTING_RECORD);
        PQclear(res);
        return;
    } 
    PQclear(res);
    sprintf(param, "INSERT INTO tags VALUES(%d,%d,'%s');",
                             atoi(photo_id), atoi(user_id), info);
    PQexec(conn, param);
}   

void* photosTags ()
{
	PGresult *res;
    char param[200];
    sprintf(param, "SELECT * FROM tags");
    res = PQexec(conn, param);
    if (!PQntuples(res))
    {
        printf(EMPTY);
        PQclear(res);	
        return;
    }
    PQclear(res);
}

int main(int argc, char const *argv[])
{
    char connect_param[200];
    sprintf(connect_param,
            "host=csl2.cs.technion.ac.il dbname=%s user=%s password=%s",
            USERNAME, USERNAME, PASSWORD);
    conn = PQconnectdb(connect_param);

    parseInput();

    PQfinish(conn);
    return 0;
}
