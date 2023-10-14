add_custom_command(
	OUTPUT lex_sql.cpp lex_sql.h
	COMMAND flex --outfile lex_sql.cpp --header-file=lex_sql.h ${CMAKE_CURRENT_SOURCE_DIR}/lex_sql.l
	DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/lex_sql.l
)
add_custom_command(
	OUTPUT yacc_sql.cpp yacc_sql.h
	COMMAND bison -d --output yacc_sql.cpp ${CMAKE_CURRENT_SOURCE_DIR}/yacc_sql.y
	DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/yacc_sql.y
)

add_library(sql_parser STATIC lex_sql.cpp yacc_sql.cpp)