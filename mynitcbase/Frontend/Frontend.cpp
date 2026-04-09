#include "Frontend.h"
#include <cstring>
#include <iostream>

#include "../trace_macros.h"

int Frontend::create_table(char relname[ATTR_SIZE], int no_attrs, char attributes[][ATTR_SIZE],
                           int type_attrs[])
{
  TRACE_FUNC("Frontend");
  return Schema::createRel(relname, no_attrs, attributes, type_attrs);
}

int Frontend::drop_table(char relname[ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  return Schema::deleteRel(relname);
}

int Frontend::open_table(char relname[ATTR_SIZE]) // ? %
{
  TRACE_FUNC("Frontend");
  return Schema::openRel(relname);
}

int Frontend::close_table(char relname[ATTR_SIZE]) // ? %
{
  TRACE_FUNC("Frontend");
  return Schema::closeRel(relname);
}

int Frontend::alter_table_rename(char relname_from[ATTR_SIZE], char relname_to[ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  return Schema::renameRel(relname_from, relname_to);
}

int Frontend::alter_table_rename_column(char relname[ATTR_SIZE], char attrname_from[ATTR_SIZE],
                                        char attrname_to[ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  return Schema::renameAttr(relname, attrname_from, attrname_to);
}

int Frontend::create_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  return Schema::createIndex(relname, attrname);
}

int Frontend::drop_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  return Schema::dropIndex(relname, attrname);
}

int Frontend::insert_into_table_values(char relname[ATTR_SIZE], int attr_count, char attr_values[][ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  return Algebra::insert(relname, attr_count, attr_values);
}

int Frontend::select_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  return Algebra::project(relname_source, relname_target);
}

int Frontend::select_attrlist_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                         int attr_count, char attr_list[][ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  return Algebra::project(relname_source, relname_target, attr_count, attr_list);
}

int Frontend::select_from_table_where(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                      char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  return Algebra::select(relname_source, relname_target, attribute, op, value);
}

int Frontend::select_attrlist_from_table_where(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                               int attr_count, char attr_list[][ATTR_SIZE],
                                               char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  int ret = Algebra::select(relname_source, TEMP, attribute, op, value);
  if (ret != SUCCESS)
    return ret;
  int relId = OpenRelTable::openRel(TEMP);
  if (relId < 0)
  {
    Schema::deleteRel(TEMP);
    return relId;
  }
  ret = Algebra::project(TEMP, relname_target, attr_count, attr_list);
  Schema::closeRel(TEMP);
  Schema::deleteRel(TEMP);
  return ret;
}

int Frontend::select_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                     char relname_target[ATTR_SIZE],
                                     char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  return Algebra::join(relname_source_one, relname_source_two, relname_target, join_attr_one, join_attr_two);
}

int Frontend::select_attrlist_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                              char relname_target[ATTR_SIZE],
                                              char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE],
                                              int attr_count, char attr_list[][ATTR_SIZE])
{
  TRACE_FUNC("Frontend");
  int ret = Algebra::join(relname_source_one, relname_source_two, TEMP, join_attr_one, join_attr_two);
  if (ret != SUCCESS)
    return ret;
  int relId = OpenRelTable::openRel(TEMP);
  if (relId < 0)
  {
    Schema::deleteRel(TEMP);
    return relId;
  }
  Algebra::project(TEMP, relname_target, attr_count, attr_list);
  OpenRelTable::closeRel(relId);
  Schema::deleteRel(TEMP);
  return SUCCESS;
}

int Frontend::custom_function(int argc, char argv[][ATTR_SIZE])
{
  // argc gives the size of the argv array
  // argv stores every token delimited by space and comma

  // implement whatever you desire

  return SUCCESS;
}
