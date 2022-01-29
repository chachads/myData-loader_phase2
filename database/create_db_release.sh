#!/bin/bash
echo "LAUNCH DB RELEASE SCRIPT"
CODE_DIR="/Users/chachads/IdeaProjects/myData-loader/database/"
RELEASE_DIR="/Users/chachads/IdeaProjects/myData-loader/database/release/"
echo "DELETING $RELEASE_DIR if exists."
if [ -d "$RELEASE_DIR" ]; then rm -Rf $RELEASE_DIR; fi
echo "Creating $RELEASE_DIR"
mkdir $RELEASE_DIR;
cat $CODE_DIR/tables/*/*.* > $RELEASE_DIR/1_tables.sql;
cat $CODE_DIR/constraints/*.* > $RELEASE_DIR/2_constraints.sql;
cat $CODE_DIR/functions/*.* > $RELEASE_DIR/3_functions.sql;
cat $CODE_DIR/views/*.* > $RELEASE_DIR/4_views.sql;
cat $CODE_DIR/defaultdata/*.* > $RELEASE_DIR/5_defaultdata.sql;
cat $RELEASE_DIR/*.* > $RELEASE_DIR/recreate_db.sql;


