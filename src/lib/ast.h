/*
No copyright is claimed in the United States under Title 17, U.S. Code.
All Other Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
#ifndef AST_H_
#define AST_H_ 1

#include <cstdio>
#include <cstdlib>
#include <inttypes.h>
#include <list>
#include <deque>
#include <vector>
#include <string>
#include <string.h>
#include <map>


#include "parse_graph.h"

class ASTVar;
class ASTFuncDecl;
class ASTKidDef;
class SymbolTable;

class StreamVar
{
public:
     typedef enum { UNSET, SOURCE, DEST } VarType_t;

     StreamVar() : name(NULL), node(NULL) { }


     std::vector<ASTVar *>& getList(VarType_t type) {
          if ( type == UNSET ) {
               error_print("Attempting to use a StreamVar that has its type unset");
          }
          return (type == SOURCE) ? sources : dests;
     }

     parse_node_var_t* getParseNode() { if ( !node ) node->name = ""; return node; }

     const char* getName() const { return name; }

private:
     void addReference(ASTVar* var, VarType_t type);
     bool removeReference(ASTVar* var, VarType_t type);
     void updateParseNode(parse_graph_t *pg);


     friend class SymbolTable;
     friend class ASTFuncDecl;
     const char *name;
     parse_node_var_t *node;
     std::vector<ASTVar *> sources;
     std::vector<ASTVar *> dests;
};


class SymbolTable
{
     SymbolTable *outer;

     typedef std::map<std::string, StreamVar> VarMap_t;
     typedef std::map<std::string, ASTFuncDecl*> FuncMap_t;
     VarMap_t vars;
     FuncMap_t funcs;

     std::vector<std::string> externVars;

     parse_graph_t *pg;


     bool isExtern(const std::string &name);

public:
     SymbolTable() : outer(NULL), pg(NULL) { }
     SymbolTable(SymbolTable *outer) : outer(outer), pg(outer->pg) { }

     void registerFunctionDecl(ASTFuncDecl* def);
     StreamVar* registerStreamVariable(ASTVar *var, StreamVar::VarType_t type);
     void deregisterStreamVariable(ASTVar *var, StreamVar::VarType_t type);
     void markVarExtern(ASTVar *var);

     ASTFuncDecl* findFunction(const std::string &name);
     StreamVar* findStreamVariable(const std::string &name);
     StreamVar* findStreamVariable(const ASTVar* var);

     void setParseGraph(parse_graph_t *parseGraph);
     parse_graph_t* getParseGraph();
};


class ASTNode;


struct ASTWalker {
     ASTWalker() : depth(0) { }
     uint32_t depth;
     virtual void operator()(ASTNode *n) = 0;
     virtual bool rootFirst() { return true; }
     virtual bool shouldStop() { return false; }
};


struct ASTDispatch;


class ASTNode
{
public:

     typedef std::deque<ASTNode*>::iterator node_iterator;
     typedef std::deque<ASTNode*>::const_iterator node_const_iterator;

     ASTNode() { }

     virtual ~ASTNode() {
          while ( !children.empty() ) {
               ASTNode *child = children[0];
               children.pop_front();
               if ( child ) delete child;
          }
     }

     const char *asString() { this->repr(); return reprBuf; }
     virtual bool isNull() const { return false; }

     void addChild(ASTNode* c) { children.push_back(c); }
     void insertChild(ASTNode* c) { children.push_front(c); }
     bool hasChildren() const { return !children.empty(); }
     size_t childCount() const { return children.size(); }
     ASTNode* getFirstChild() { return children.front(); }
     ASTNode* getLastChild() { return children.back(); }
     std::deque<ASTNode*>& getChildren() { return children; }


     void walk(ASTWalker &walker);
     virtual void *dispatch(ASTDispatch &target);

     virtual bool verify() const { return true; }

     bool verifyTree();

protected:
     /* TODO:  Make static, and place in a .cc */
     char reprBuf[4096];
     /** Returns a string representing the representation of this node */
     virtual void repr() { sprintf(reprBuf, "Root Node"); }
     std::deque<ASTNode*> children;

};



class ASTNULL : public ASTNode
{
public:
     ASTNULL() : ASTNode() { }
     virtual bool isNull() const { return true; }
     virtual void repr() { strcpy(reprBuf, "NULL"); }
     virtual void *dispatch(ASTDispatch &target);
     ~ASTNULL() { }
};


class ASTStatementList : public ASTNode
{
public:
     ASTStatementList() : ASTNode() { }
     ~ASTStatementList() { }
     virtual void *dispatch(ASTDispatch &target);
     virtual void repr() { strcpy(reprBuf, "StatementList"); }
};



class ASTKidDef : public ASTNode
{
public:
     typedef enum {NONE, PIPE, DOUBLEPIPE} KIDOUT;

     ASTKidDef(char *tok) : ASTNode(), sourcePort(NULL), inType(NONE), graphNode(NULL)
     {
          tokens.push_back(tok);
     }

     ASTKidDef(long long num) : ASTNode(), sourcePort(NULL), inType(NONE), graphNode(NULL)
     {
          char tmp[32];
          sprintf(tmp, "%lld", num);
          tokens.push_back(strdup(tmp));
     }
     ~ASTKidDef() {
          while ( !tokens.empty() ) {
               free(tokens.back());
               tokens.pop_back();
          }
          if ( sourcePort ) free(sourcePort);
     }

     virtual void *dispatch(ASTDispatch &target);
     virtual void repr() {
          char *p = reprBuf;
          p += sprintf(p, "ASTKidDef: ");
          for ( std::list<char*>::iterator i = tokens.begin() ; i != tokens.end() ; ++i ) {
               p += sprintf(p, "%s ", (*i));
          }
     }

     void setSourcePort(char * port)
     {
          sourcePort = port;
     }

     const char* getSourcePort(void) const { return sourcePort; }
     const char* getKidName(void) const { return tokens.front(); }

     void prefaceItem(char *tok)
     {
          tokens.push_front(tok);
     }


     void prefaceItem(long long num)
     {
          char tmp[32];
          sprintf(tmp, "%lld", num);
          tokens.push_front(strdup(tmp));
     }

     void setInPipeType(KIDOUT t) { inType = t; }
     KIDOUT getInPipeType() const { return inType; }

     const std::list<char *>& getTokens() const { return tokens; }

     void setParseNode(parse_node_proc_t *node) { graphNode = node; }
     parse_node_proc_t *getParseNode() const { return graphNode; }

protected:
     char * sourcePort;
     KIDOUT inType;
     std::list<char *> tokens;
     parse_node_proc_t *graphNode;
};


class ASTKidList : public ASTNode
{
public:
     ASTKidList() : ASTNode() { }
     ASTKidList(ASTNode *kid) : ASTNode()
     {
          addChild(kid);
     }
     ~ASTKidList() { }
     ASTKidDef* getKid(size_t n) { return static_cast<ASTKidDef*>(children[n]); }
     virtual void *dispatch(ASTDispatch &target);
     virtual void repr() { snprintf(reprBuf, 255, "ASTKidList"); }
};


class ASTVar : public ASTNode
{
public:
     ASTVar(char *name, SymbolTable *symtab) :
          ASTNode(), symtab(symtab), name(name), fullName(NULL),
          filter(NULL), target(NULL), bundled(false), svar(NULL), svMode(StreamVar::UNSET)
     {
          setVersion(0);
     }
     ~ASTVar() {
          if ( name ) free(name);
          if ( fullName ) free(fullName);
          if ( target ) free(target);
          if ( filter ) free(filter);
     }
     virtual void *dispatch(ASTDispatch &target);

     void setSink(ASTNode *source, bool regVar=true) {
          svMode = StreamVar::SOURCE;
          if ( regVar) svar = symtab->registerStreamVariable(this, StreamVar::SOURCE);
          targetKid = static_cast<ASTKidDef*>(source);
     }

     void setSource(ASTNode *source, bool regVar=true) {
          svMode = StreamVar::DEST;
          if ( regVar) svar = symtab->registerStreamVariable(this, StreamVar::DEST);
          targetKid = static_cast<ASTKidDef*>(source);
     }

     void deregisterStream() {
          symtab->deregisterStreamVariable(this, svMode);
          svar = NULL;
     }

     void setExtern() {
          symtab->markVarExtern(this);
     }

     virtual void repr()
     {
          char *buf = reprBuf;
          buf += sprintf(buf, "Var[%s]", name);
          if ( filter ) {
               buf += sprintf(buf, ".%s", filter);
          }
          if ( target ) {
               buf += sprintf(buf, ":%s", target);
          }
     }

     void setVersion(uint32_t v, bool regVar=false)
     {
          if ( !fullName ) {
               fullName = (char*)calloc(strlen(name) + 12, sizeof(char));
          }
          bool reRegister = (svar != NULL);
          if ( reRegister ) symtab->deregisterStreamVariable(this, svMode);
          sprintf(fullName, "%s_f%08x", name, v);
          if ( reRegister || regVar ) svar = symtab->registerStreamVariable(this, svMode);
     }

     void reName(char *newName) {
          if ( svar ) {
               /* We've been registered */
               symtab->deregisterStreamVariable(this, svMode);
               svar = NULL;
               if ( fullName ) { free(fullName); fullName = NULL; }
               if ( name ) free(name);
               name = newName;
               setVersion(0, true);
          } else {
               if ( name ) free(name);
               name = newName;
          }
     }

     const char* getName() const { return name; }
     const char* getFullName() const { return fullName; }
     ASTKidDef* getTargetKid() const { return targetKid; }
     void setFilter(char *f) { filter = f; }
     const char* getFilter() const { return filter; }
     void setTargetPort(char *t) { target = t; }
     const char* getTargetPort() const { return target; }
     void setBundle() { bundled = true; }
     bool isBundled() const { return bundled; }
     StreamVar* getStreamVar() {
          if ( !svar ) return symtab->findStreamVariable(this);
          return svar;
     }
     SymbolTable* getSymtab() { return symtab; }

protected:
     SymbolTable *symtab;
     char *name;
     char *fullName;
     char *filter;
     char *target;
     bool bundled;
     StreamVar *svar;
     StreamVar::VarType_t svMode;
     ASTKidDef * targetKid;
};


class ASTVarList : public ASTNode
{
public:
     ASTVarList() : ASTNode()
     {
     }

     ASTVarList(ASTNode *var) : ASTNode()
     {
          addChild(var);
     }
     ~ASTVarList() { }
     virtual void *dispatch(ASTDispatch &target);
     virtual void repr() { snprintf(reprBuf, 255, "ASTVarList"); }
     ASTVar* getVar(size_t n) { return static_cast<ASTVar*>(children[n]); }

     void setVersion(uint32_t version, bool regVar=false)
     {
          size_t nC = children.size();
          for ( size_t i = 0 ; i < nC ; i++ ) {
               getVar(i)->setVersion(version, regVar);
          }
     }

     void deregisterStream()
     {
          size_t nC = children.size();
          for ( size_t i = 0 ; i < nC ; i++ ) {
               getVar(i)->deregisterStream();
          }
     }
};



class ASTThreadDecl : public ASTNode
{
public:
     ASTThreadDecl(unsigned long long tid, bool twoD, ASTNode *body) : ASTNode(), tid(tid), twoD(twoD)
     {
          addChild(body);
     }
     ~ASTThreadDecl() { }
     virtual void *dispatch(ASTDispatch &target);
     virtual void repr() { snprintf(reprBuf, 255, "Thread(%llu)", tid); }
     unsigned long long tid;
     bool twoD;
     ASTStatementList* getBody() { return static_cast<ASTStatementList*>(children[0]); }
};


class ASTFuncDecl : public ASTNode
{
public:
     ASTFuncDecl(char *name, SymbolTable *origTab) : ASTNode(), name(name)
     {
          number = ASTFuncDecl::getFunctionNumber();
          addChild(new ASTNULL());
          addChild(new ASTNULL());
          addChild(new ASTNULL());
          origTab->registerFunctionDecl(this);
          symtab = new SymbolTable(origTab);
          recursionDetect = false;
     }
     ~ASTFuncDecl() {
          if (name) free(name);
          if (symtab) delete symtab;
     }
     virtual void *dispatch(ASTDispatch &target);
     virtual void repr() { snprintf(reprBuf, 255, "Func %s", name); }

     void addSource(ASTNode *node) {
          ASTVarList* src = dynamic_cast<ASTVarList*>(node);

          delete children[0];
          children[0] = node;

          size_t nC = src->childCount();
          for ( size_t i = 0 ; i < nC ; i++ ) {
               src->getVar(i)->setSource(NULL);
          }
     }
     void addDests(ASTNode *node) {
          ASTVarList* dst = dynamic_cast<ASTVarList*>(node);

          delete children[1];
          children[1] = node;

          size_t nC = dst->childCount();
          for ( size_t i = 0 ; i < nC ; i++ ) {
               dst->getVar(i)->setSink(NULL);
          }
     }

     void addStatements(ASTNode *node) { delete children[2]; children[2] = node; }

     /**
      * Called to fix up miscellaneous "bad" things that may be in the function
      */
     void fixup();

     ASTVarList* getSources() { return static_cast<ASTVarList*>(children[0]); }
     ASTVarList* getDests() { return static_cast<ASTVarList*>(children[1]); }
     ASTStatementList* getBody() { return static_cast<ASTStatementList*>(children[2]); }
     const char* getName() const { return name; }
     uint32_t getFuncNumber() const { return number; }

     SymbolTable *getSymbolTable() { return symtab; }

     /*
      * Call these from an ASTFuncCall
      */
     typedef std::vector<parse_node_var_t*> nodeList_t;
     typedef struct {
          bool success;
          nodeList_t inList;
          nodeList_t outList;
          std::vector<ASTVar*> filterResets;
     } paramList_t;

     paramList_t setupParameters(ASTVarList *in, ASTVarList *out, uint32_t version, parse_graph_t *pg);
     void resetParameters(paramList_t& params);

     virtual bool verify() const
     {
          return !(children[0]->isNull() || children[1]->isNull() || children[2]->isNull());
     }

protected:
     char *name;
     uint32_t number;
     SymbolTable *symtab;
     bool recursionDetect;

private:
     static uint32_t funcCount;
     static uint32_t getFunctionNumber() { return ++funcCount; }
};


class ASTFuncCall : public ASTNode
{
public:
     ASTFuncCall(char *name, ASTNode *sources, ASTNode *dests, SymbolTable *symtab) :
          ASTNode(), name(name), symtab(symtab)
     {
          addChild(sources);
          addChild(dests);

          size_t nC = getSources()->childCount();
          for ( size_t i = 0 ; i < nC ; i++ ) {
               getSources()->getVar(i)->setSink(NULL);
          }

          nC = getDests()->childCount();
          for ( size_t i = 0 ; i < nC ; i++ ) {
               getDests()->getVar(i)->setSource(NULL);
          }

     }
     ~ASTFuncCall() { free(name); }
     virtual void *dispatch(ASTDispatch &target);
     virtual void repr() { snprintf(reprBuf, 255, "Func Call %s", name); }

     ASTVarList* getSources() { return static_cast<ASTVarList*>(children[0]); }
     ASTVarList* getDests() { return static_cast<ASTVarList*>(children[1]); }
     const char* getName() const { return name; }

protected:
     char* name;
     SymbolTable *symtab;
};


class ASTPipeline : public ASTNode
{
public:
     ASTPipeline(ASTNode *sources, ASTNode *kidList, ASTNode *sink, bool registerVars) : ASTNode()
     {
          addChild(sources ? sources : new ASTNULL() );
          addChild(kidList);
          addChild(sink ? sink : new ASTNULL() );
          registerStreamVars(registerVars);
     }
     ~ASTPipeline() { }
     virtual void *dispatch(ASTDispatch &target);
     virtual void repr() { sprintf(reprBuf, "Pipeline"); }

     ASTVarList* getSources() { return static_cast<ASTVarList*>(children[0]); }
     ASTKidList* getPipeline() { return static_cast<ASTKidList*>(children[1]); }
     ASTVar* getSink() { return static_cast<ASTVar*>(children[2]); }

     void registerStreamVars(bool regVar)
     {
          if ( !getSink()->isNull() ) {
               getSink()->setSink(static_cast<ASTKidDef*>(getPipeline()->getLastChild()), regVar);
          }
          if ( !getSources()->isNull() ) {
               ASTVarList *vars = getSources();
               for ( ASTNode::node_iterator i = vars->getChildren().begin() ; i != vars->getChildren().end() ; ++i ) {
                    ASTVar* v = static_cast<ASTVar*>(*i);
                    v->setSource(static_cast<ASTKidDef*>(getPipeline()->getFirstChild()), regVar);
               }
          }
     }

};



struct ASTDispatch {
     virtual void* processNode(ASTNode &node)          { return NULL; }
     virtual void* processNode(ASTNULL &node)          { return NULL; }
     virtual void* processNode(ASTStatementList &node) { return NULL; }
     virtual void* processNode(ASTKidDef &node)        { return NULL; }
     virtual void* processNode(ASTKidList &node)       { return NULL; }
     virtual void* processNode(ASTVar &node)           { return NULL; }
     virtual void* processNode(ASTVarList &node)       { return NULL; }
     virtual void* processNode(ASTThreadDecl &node)    { return NULL; }
     virtual void* processNode(ASTFuncDecl &node)      { return NULL; }
     virtual void* processNode(ASTFuncCall &node)      { return NULL; }
     virtual void* processNode(ASTPipeline &node)      { return NULL; }
};

#endif /* AST_H_ */
