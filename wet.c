#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>

#include "wet.h"

static PGconn *conn;

static void printErr(const char *message) {
    fprintf(stderr, "%s: %s\n", message, PQerrorMessage(conn));
}

void *addUser(const char *name) {
    const char *params[1] = {name};
    const int lens[1] = {(int) strlen(name)};
    const int fmts[1] = {0};

    PGresult *res = PQexecParams(
        conn,
        "INSERT INTO users (id, name) "
        "VALUES "
            "((SELECT coalesce(max(id) + 1, 0) FROM users), $1::text) "
        "RETURNING id;",
        1,
        NULL, // Types - deduce from query
        params,
        lens,
        fmts, // Formats - all text
        0 // Result - in text
    );

    if (PQresultStatus(res) == PGRES_TUPLES_OK)
        printf(ADD_USER, PQgetvalue(res, 0, 0));
    else
        printErr("Failed to add user");

    PQclear(res);

    return NULL;
}
/*select MIN(id + 1) from users where id + 1 <> ALL (select id from users);
*/
void *addUserMin(const char *name) {
    const char *params[1] = {name};
    const int lens[1] = {(int) strlen(name)};
    const int fmts[1] = {0};

    PGresult *res = PQexecParams(
        conn,
        "INSERT INTO users (id, name) "
        "VALUES "
            "((SELECT coalesce(min(id + 1), 0) "
                "FROM users "
                "WHERE id + 1 NOT IN (SELECT id FROM users) "
                "), $1::text);",
        1,
        NULL, // Types - deduce from query
        params,
        lens,
        fmts, // Formats - all text
        0 // Result - in text
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        printErr("Failed to add user");
    PQclear(res);

    res = PQexecParams(
        conn,
        "SELECT id, name FROM users WHERE name = $1::text ORDER BY id;",
        1,
        NULL, // Types - deduce from query
        params,
        lens,
        fmts, // Formats - all text
        0 // Result - in text
    );

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printErr("Could not fetch users of same name");
    } else {
        int i;
        printf(USER_HEADER);
        for (i = 0; i < PQntuples(res); i++) {
            printf(
                USER_RESULT,
                PQgetvalue(res, i, 0),
                PQgetvalue(res, i, 1)
            );
        }
    }
    PQclear(res);

    return NULL;
}

void *removeUser(const char *id) {
    const char *params[1] = {id};
    const int lens[1] = {(int) strlen(id)};
    const int fmts[1] = {0};

    PGresult *res = PQexecParams(
        conn,
        "DELETE FROM users WHERE id = $1::integer;",
        1,
        NULL, // Types - deduce from query
        params,
        lens,
        fmts, // Formats - all text
        0 // Result - in text
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printErr("Failed to remove used");
    } else {
        // Read number of affected rows, no row => no such user
        if (strtol(PQcmdTuples(res), NULL, 10) == 0) {
            printf(ILL_PARAMS);
        }
    }
    PQclear(res);

    res = PQexecParams(
        conn,
        "DELETE FROM photos WHERE user_id = $1::integer;",
        1, NULL, params, lens, fmts, 0
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        printErr("Failed to remove user's photos");
    PQclear(res);

    res = PQexecParams(
        conn,
        "DELETE FROM tags WHERE user_id = $1::integer;",
        1, NULL, params, lens, fmts, 0
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        printErr("Failed to remove user's tags");
    PQclear(res);

    return NULL;
}

void *addPhoto(const char *user_id, const char *photo_id) {
    PGresult *res;
    char param[200];
    sprintf(param, "SELECT * FROM users WHERE id = %d", atoi(user_id));
    res = PQexec(conn, param);
    if (!PQntuples(res))
    {
        printf(ILL_PARAMS);
        PQclear(res);
        return NULL;
    }
    PQclear(res);
    sprintf(param, "SELECT * FROM photos WHERE user_id = %d AND id = %d;",
                                            atoi(user_id), atoi(photo_id));
    res = PQexec(conn, param);
    if (PQntuples(res))
    {
        printf(EXISTING_RECORD);
        PQclear(res);
        return NULL;
    }
    PQclear(res);
    sprintf(param, "INSERT INTO photos VALUES(%d,%d);",
                                           atoi(photo_id), atoi(user_id));
    PQexec(conn, param);

    return NULL;
}

void *tagPhoto(const char *user_id, const char *photo_id, const char *info) {
    PGresult *res;
    char param[200];
    sprintf(param, "SELECT * FROM users WHERE id = %d", atoi(user_id));
    res = PQexec(conn, param);
    if (!PQntuples(res))
    {
        printf(ILL_PARAMS);
        PQclear(res);
        return NULL;
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
        return NULL;
    }
    PQclear(res);
    sprintf(param, "INSERT INTO tags VALUES(%d,%d,'%s');",
                             atoi(photo_id), atoi(user_id), info);
    PQexec(conn, param);

    return NULL;
}

void *photosTags() {
    PGresult *res;
    res = PQexec(
        conn,
        "SELECT p.user_id, p.id, COUNT(t.info) AS count "
        "FROM photos AS p "
        "LEFT JOIN tags AS t ON p.id = t.photo_id AND p.user_id = t.user_id "
        "GROUP BY p.user_id, p.id "
        "ORDER BY count DESC, p.user_id, p.id;"
    );

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printErr("Failed to search for tags");
    } else {
        if (PQntuples(res) == 0) {
            printf(EMPTY);
        } else {
            printf(PHOTOS_HEADER);
            int i;
            for (i = 0; i < PQntuples(res); i++) {
                printf(
                    PHOTOS_RESULT,
                    PQgetvalue(res, i, 0),
                    PQgetvalue(res, i, 1),
                    PQgetvalue(res, i, 2)
                );
            }
        }
    }

    PQclear(res);
    return NULL;
}

void *search(const char *qword) {
    const char *params[1] = {qword};
    const int lens[1] = {(int) strlen(qword)};
    const int fmts[1] = {0};

    PGresult *res = PQexecParams(
        conn,
        "SELECT user_id, photo_id, count(info) AS count "
        "FROM tags "
        "WHERE info LIKE ('%' || $1::text || '%')"
        "GROUP BY user_id, photo_id "
        "ORDER BY count DESC, user_id, photo_id;",
        1, NULL, params, lens, fmts, 0
    );

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printErr("Failed to search for tags");
    } else {
        if (PQntuples(res) == 0) {
            printf(EMPTY);
        } else {
            printf(PHOTOS_HEADER);
            int i;
            for (i = 0; i < PQntuples(res); i++) {
                printf(
                    PHOTOS_RESULT,
                    PQgetvalue(res, i, 0),
                    PQgetvalue(res, i, 1),
                    PQgetvalue(res, i, 2)
                );
            }
        }
    }

    PQclear(res);
    return NULL;
}

static void printCommon(PGresult *res) {
    if (PQntuples(res) == 0) {
        printf(EMPTY);
        return;
    }

    printf(COMMON_HEADER);
    int i;
    for (i = 0; i < PQntuples(res); i++) {
        printf(
            COMMON_LINE,
            PQgetvalue(res, i, 0),
            PQgetvalue(res, i, 1)
        );
    }
}

void *commonTags(const char *k) {
    const char *params[1] = {k};
    const int lens[1] = {(int) strlen(k)};
    const int fmts[1] = {0};
    PGresult *res = PQexecParams(
        conn,
        "SELECT info, count(DISTINCT (user_id::text || '/' || photo_id::text)) "
        "FROM tags "
        "GROUP BY info "
        "HAVING count(DISTINCT (user_id::text || '/' || photo_id::text)) >= $1::integer "
        "ORDER BY count DESC, info DESC",
        1, NULL, params, lens, fmts, 0
    );

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        printErr("Failed to find common tags");
    else
        printCommon(res);

    PQclear(res);
    return NULL;
}

void *mostCommonTags(const char *k) {
    return NULL;
}


void *similarPhotos(const char *k, const char *j) {
    const char *params[2] = {k, j};
    const int lens[2] = {(int) strlen(k), (int) strlen(j)};
    const int fmts[2] = {0, 0};
    PGresult *res = PQexecParams(
        conn,
        "SELECT q.user_id, u.name, q.photo_id FROM ( "
            "SELECT t.user_id, t.photo_id, count(DISTINCT t.info) "
            "FROM tags AS t "
            "INNER JOIN tags AS tt ON t.info = tt.info "
            "WHERE (t.user_id != tt.user_id OR (t.user_id = tt.user_id AND t.photo_id != tt.photo_id)) "
            "GROUP BY t.user_id, t.photo_id, tt.user_id, tt.photo_id "
            "HAVING count(t.info) >= $2::integer "
        ") AS q "
        "INNER JOIN users AS u ON u.id = q.user_id "
        "GROUP BY q.user_id, q.photo_id, u.name "
        "HAVING count(q.count) >= $1::integer "
        "ORDER BY q.user_id, q.photo_id;",
        2, NULL, params, lens, fmts, 0
    );

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        printErr("Failed to find similar photos");
    } else {
        if (PQntuples(res) == 0) {
            printf(EMPTY);
        } else {
            printf(SIMILAR_HEADER);
            int i;
            for (i = 0; i < PQntuples(res); i++) {
                printf(
                    SIMILAR_RESULT,
                    PQgetvalue(res, i, 0),
                    PQgetvalue(res, i, 1),
                    PQgetvalue(res, i, 2)
                );
            }

        }
    }
    PQclear(res);
    return NULL;
}

static int execute(const char *stmt) {
    PGresult *res = PQexec(conn, stmt);
    int ret = PQresultStatus(res) == PGRES_COMMAND_OK;
    PQclear(res);
    return ret;
}

void *autoPhotoOnTagOn() {
    autoPhotoOnTagOFF();
    if (!execute(
        "CREATE OR REPLACE FUNCTION autoadd_photo() \n"
        "RETURNS TRIGGER AS $$ \n"
            "BEGIN \n"
                "IF (NEW.user_id IN (SELECT id FROM users WHERE id = NEW.user_id)) THEN \n"
                    "IF (NEW.photo_id NOT IN (SELECT id FROM photos WHERE user_id = NEW.user_id)) THEN \n"
                        "INSERT INTO photos (id, user_id) VALUES (NEW.photo_id, NEW.user_id); \n"
                    "END IF; \n"
                "END IF; \n"
            "RETURN NEW; \n"
            "END; \n"
        "$$ LANGUAGE plpgsql;"
    )) {
        printErr("Failed to create trigger procedure");
        return NULL;
    }

    if (!execute(
        "CREATE TRIGGER autoadd_photo \n"
        "BEFORE INSERT ON tags \n"
        "FOR EACH ROW EXECUTE PROCEDURE autoadd_photo();"
    )) {
        printErr("Failed to add trigger to tags table");
        return NULL;
    }

    return NULL;
}

void *autoPhotoOnTagOFF() {
    PGresult *res = PQexec(conn, "DROP TRIGGER IF EXISTS autoadd_photo ON tags;");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printErr("Failed to remove trigger");
    }
    PQclear(res);
    return NULL;
}


int main() {
    char connect_param[200];
    sprintf(connect_param,
            "host=csl2.cs.technion.ac.il dbname=%s user=%s password=%s",
            USERNAME, USERNAME, PASSWORD);
    conn = PQconnectdb(connect_param);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(
            stderr,
            "Failed to connect to the database: %s\n",
            PQerrorMessage(conn)
        );
        return 1;
    }

    if (!execute("DROP LANGUAGE IF EXISTS plpgsql CASCADE;")) {
        printErr("Failed to drop language");
        return 1;
    }
    if (!execute("CREATE LANGUAGE plpgsql;")) {
        printErr("Failed to create language");
        return 1;
    }
    parseInput();

    PQfinish(conn);
    return 0;
}
