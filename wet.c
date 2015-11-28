#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include "wet.h"

static PGconn *conn;

// INSERT INTO users(id, name)
// VALUES
//  (1 + (SELECT max(id) FROM users), %s)
// RETURNING id;
void *addUser(const char *name) {
    const char *params[1] = {name};
    const int lens[1] = {(int) strlen(name)};
    const int fmts[1] = {0};

    PGresult *res = PQexecParams(
        conn,
        "INSERT INTO users (id, name)"
        "VALUES"
        "   (1 + (SELECT max(id) FROM users), $1::text)"
        "RETURNING id;",
        1,
        NULL, // Types - deduce from query
        params,
        lens,
        fmts, // Formats - all text
        0 // Result - in text
    );

    if (PQresultStatus(res) == PGRES_TUPLES_OK) {
        printf(ADD_USER, PQgetvalue(res, 0, 0));
    } else {
        fprintf(stderr, "Failed to add user: %s", PQerrorMessage(conn));
    }
    PQclear(res);

    return NULL;
}

void *addUserMin(const char *name) {
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

    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        // Read number of affected rows
        if (strtol(PQcmdTuples(res), NULL, 10) == 0)
            printf(ILL_PARAMS);
    } else {
        fprintf(stderr, "Failed to remove user: %s", PQerrorMessage(conn));
    }
    PQclear(res);
    // FIXME from other tables as well

    return NULL;
}

void *addPhoto(const char *user_id, const char *photo_id) {
    return NULL;
}

void *tagPhoto(const char *user_id, const char *photo_id, const char *info) {
    return NULL;
}

void *photosTags() {
    return NULL;
}

void *search(const char *qword) {
    return NULL;
}

void *commonTags(const char *k) {
    return NULL;
}

void *mostCommonTags(const char *k) {
    return NULL;
}

void *similarPhotos(const char *k, const char *j) {
    return NULL;
}

void *autoPhotoOnTagOn() {
    return NULL;
}

void *autoPhotoOnTagOFF() {
    return NULL;
}


int main(int argc, char **argv) {
    char connect_param[200];

    sprintf(
        connect_param,
        "host=csl2.cs.technion.ac.il dbname=%s user=%s password=%s",
        USERNAME,
        USERNAME,
        PASSWORD
    );

    conn = PQconnectdb(connect_param);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(
            stderr,
            "Failed to connect to the database: %s\n",
            PQerrorMessage(conn)
        );
        return 1;
    }
    parseInput();
    return 0;
}
