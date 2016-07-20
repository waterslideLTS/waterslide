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
#include "ast.h"
#include "error_print.h"


/******** StreamVar ********/
void StreamVar::addReference(ASTVar* var, VarType_t type)
{
     if ( !name ) {
          name = var->getFullName();
     }
     getList(type).push_back(var);
     //fprintf(stderr, "SVar Add Reference: %s [%zu : %zu]\n", var->getFullName(), sources.size(), dests.size());
}


bool StreamVar::removeReference(ASTVar *var, VarType_t type)
{
     std::vector<ASTVar *> &list = getList(type);
     std::vector<ASTVar *>::iterator pos;
     for ( pos = list.begin() ; pos != list.end() ; ++pos ) {
          if ( *pos == var ) {
               list.erase(pos);
               break;
          }
     }

     //fprintf(stderr, "SVar: Remove Reference: %s [%zu : %zu]\n", var->getFullName(), sources.size(), dests.size());

     /* If no more references remain, return true */
     return (sources.empty() && dests.empty());
}


void StreamVar::updateParseNode(parse_graph_t *pg)
{
     if ( !node ) {
          node = (parse_node_var_t*)listhash_find_attach(pg->vars, name, strlen(name));
          node->name = strdup(name);
     }
}

/******** SymbolTable ********/

bool SymbolTable::isExtern(const std::string &name)
{
     bool res = false;
     for ( std::vector<std::string>::const_iterator i = externVars.begin() ; !res && i != externVars.end() ; ++i ) {
          if ( name == *i ) res = true;
     }

     return res;
}

void SymbolTable::registerFunctionDecl(ASTFuncDecl* def)
{
     if ( funcs.count(def->getName()) ) {
          fprintf(stderr, "ERROR:  Duplicate definition of function %s\n", def->getName());
          return;
     }
     funcs[def->getName()] = def;
}


StreamVar*  SymbolTable::registerStreamVariable(ASTVar *var, StreamVar::VarType_t type)
{
     if ( isExtern(var->getName()) ) return NULL;
     //fprintf(stderr, "SymTab: Regsitering %s [%s]\n", var->getName(), var->getFullName());
     StreamVar *svar = &vars[var->getName()];
     svar->addReference(var, type);
     if ( pg ) svar->updateParseNode(pg);
     return svar;
}

void SymbolTable::deregisterStreamVariable(ASTVar *var, StreamVar::VarType_t type)
{
     if ( isExtern(var->getName()) ) return;
     //fprintf(stderr, "SymTab: De-Regsitering %s [%s]\n", var->getName(), var->getFullName());
     VarMap_t::iterator i = vars.find(var->getName());
     if ( i != vars.end() ) {
          bool empty = i->second.removeReference(var, type);
          if ( empty ) {
               vars.erase(i);
          }
     }
}


void SymbolTable::markVarExtern(ASTVar *var)
{
     VarMap_t::iterator i = vars.find(var->getName());
     while ( i != vars.end() ) {
          vars.erase(i);
          i = vars.find(var->getName());
     }
     externVars.push_back(var->getName());
}



ASTFuncDecl* SymbolTable::findFunction(const std::string &name)
{
     FuncMap_t::iterator i = funcs.find(name);
     if ( i != funcs.end() ) return i->second;
     if ( outer != NULL ) return outer->findFunction(name);
     error_print("Unable to find Function %s", name.c_str());
     return NULL;
}


StreamVar* SymbolTable::findStreamVariable(const std::string &name)
{
     VarMap_t::iterator i = vars.find(name);
     if ( i != vars.end() ) return &(i->second);
     if ( outer != NULL ) return outer->findStreamVariable(name);
     error_print("Unable to find Stream Variable %s", name.c_str());
     return NULL;
}


StreamVar* SymbolTable::findStreamVariable(const ASTVar *v)
{
     return findStreamVariable(v->getName());
}


void SymbolTable::setParseGraph(parse_graph_t *parseGraph)
{
     pg = parseGraph;
     for ( VarMap_t::iterator i = vars.begin() ; i != vars.end() ; ++i ) {
          i->second.updateParseNode(pg);
     }
}

parse_graph_t* SymbolTable::getParseGraph()
{
     if ( pg ) return pg;
     if ( outer ) return outer->getParseGraph();
     return NULL;
}

/********  ASTNode ********/

void ASTNode::walk(ASTWalker &walker)
{
     if ( walker.rootFirst() ) walker(this);
     if ( walker.shouldStop() ) return;
     walker.depth++;
     for ( std::deque<ASTNode*>::iterator i = children.begin() ; i != children.end() ; ++i ) {
          (*i)->walk(walker);
          if ( walker.shouldStop() ) { walker.depth--; return; }
     }
     walker.depth--;
     if ( !walker.rootFirst() ) walker(this);
}



/* Private walker used for ASTNode::VerifyTree() */
struct VerifyWalker : public ASTWalker {
     bool isOK;
     VerifyWalker() : isOK(true) { }

     virtual void operator()(ASTNode *n) {
          isOK = n->verify();
     }

     virtual bool shouldStop() { return !isOK; }
};

bool ASTNode::verifyTree()
{
     VerifyWalker walker;
     walk(walker);
     return walker.isOK;
}



void ASTFuncDecl::fixup()
{
     /* 1) If a Dest argument is also used as a Pipeline source, badness
      * can occur.  Change up the name of the argument, and add a redirect to
      * prevent name clashes.
      */
     if ( !getDests()->isNull() ) {
          size_t cCount = getDests()->childCount();
          for ( size_t i = 0 ; i < cCount ; i++ ) {
               ASTVar *arg = getDests()->getVar(i);
               ASTVar *sample = NULL;
               /* Search for uses of this variable as a source */
               bool needsPatch = false;
               for ( node_iterator i = getBody()->getChildren().begin() ; !needsPatch && (i != getBody()->getChildren().end()) ; ++i ) {
                    ASTPipeline *pipe = dynamic_cast<ASTPipeline*>(*i);
                    ASTFuncCall *call = dynamic_cast<ASTFuncCall*>(*i);
                    ASTVarList *vars = NULL;
                    if ( pipe && !pipe->getSources()->isNull() ) {
                         vars = pipe->getSources();
                    } else if ( call && !call->getSources()->isNull() ) {
                         vars = call->getSources();
                    }
                    if ( vars ) {
                         size_t nVar = vars->childCount();
                         for ( size_t i = 0 ; i < nVar ; i++ ) {
                              ASTVar *v = vars->getVar(i);
                              if ( !strcmp(v->getName(), arg->getName()) ) {
                                   needsPatch = true;
                                   sample = v;
                              }
                         }
                    }
               }
               if ( needsPatch && sample ) {
                    /* Patch up the function:
                     *   $arg's name becomes $arg_outXXXXXX
                     *   Append to body:   $arg -> $arg_outXXXXXX
                     */
                    char *oldName = strdup(arg->getName());
                    size_t len = strlen(oldName) + 16;
                    char *newName = (char*)malloc(len);
                    snprintf(newName, len, "%s_out%08x", oldName, rand());

                    /* Change Name */
                    arg->reName(newName);

                    /* Build new pipe */
                    //fprintf(stderr, "Adding pipe: %s | noop -> %s\n", oldName, newName);
                    ASTVar *source = new ASTVar(oldName, sample->getSymtab());
                    ASTVarList *sVars = new ASTVarList(source);
                    ASTVar *dest = new ASTVar(strdup(newName), sample->getSymtab());
                    ASTKidDef *noop = new ASTKidDef(strdup("noop"));
                    noop->setInPipeType(ASTKidDef::PIPE);
                    ASTKidList *list = new ASTKidList();
                    list->insertChild(noop);
                    ASTPipeline *pipe = new ASTPipeline(sVars, list, dest, false);

                    getBody()->addChild(pipe);
               }
          }
     }
}


ASTFuncDecl::paramList_t ASTFuncDecl::setupParameters(ASTVarList *in, ASTVarList *out, uint32_t version, parse_graph_t *pg)
{
     paramList_t params;
     params.success = false;

     if ( recursionDetect ) {
          error_print("Metaproc Graph Functions do not support recurion!");
          return params;
     }
     recursionDetect = true;

     if ( in->childCount() != getSources()->childCount() ) {
          error_print("Function call mismatch on IN variables for function %s\n", getName());
          return params;
     }

     if ( out->childCount() != getDests()->childCount() ) {
          error_print("Function call mismatch on OUT variables for function %s\n", getName());
          return params;
     }


     /* Multi-step process:
      * 1) Reset the version number on our stream variables inside
      * 2) Re-register the parse_graph variables
      * 3) Map In and Out variables
      */

     uint32_t varVers = (getFuncNumber() << 16) | version;

     /* Reset version numbers */
     if ( getSources()->hasChildren() ) {
          getSources()->setVersion(varVers);
     }
     if ( getDests()->hasChildren() ) {
          getDests()->setVersion(varVers);
     }
     for ( node_iterator i = getBody()->getChildren().begin() ; i != getBody()->getChildren().end() ; ++i ) {
          ASTPipeline *pipe = dynamic_cast<ASTPipeline*>(*i);
          if ( pipe ) {
               if ( !pipe->getSources()->isNull() ) {
                    pipe->getSources()->setVersion(varVers, true);
               }
               if ( !pipe->getSink()->isNull() ) {
                    pipe->getSink()->setVersion(varVers, true);
               }
          } else {
               ASTFuncCall *call = dynamic_cast<ASTFuncCall*>(*i);
               if ( call ) {
                    if ( !call->getSources()->isNull() ) {
                         call->getSources()->setVersion(varVers);
                    }
                    if ( !call->getDests()->isNull() ) {
                         call->getDests()->setVersion(varVers);
                    }
               }
          }
     }



     /* Re-register the parse_graph variables */
     symtab->setParseGraph(pg);



     /* Map in & out variables */

     if ( !getSources()->isNull() ) {
          size_t cCount = getSources()->childCount();
          for ( size_t i = 0 ; i < cCount ; i++ ) {
               ASTVar *outer = in->getVar(i);
               ASTVar *inner = getSources()->getVar(i);

               StreamVar *svOutter = outer->getStreamVar();
               if ( !svOutter ) { error_print("Unable to find outer streamVar %s\n", outer->getName()); return params; }
               StreamVar *svInner = inner->getStreamVar();
               if ( !svOutter ) { error_print("Unable to find inner streamVar %s\n", inner->getName()); return params; }

               params.inList.push_back(svInner->node);
               svInner->node = svOutter->node;

               if ( outer->getFilter() != NULL ) {
                    /* User put a filter on an argument */
                    std::vector<ASTVar*> &vlist = svInner->getList(StreamVar::DEST);

                    for ( std::vector<ASTVar*>::iterator i = vlist.begin() ; i != vlist.end() ; ++i ) {
                         ASTVar *v = (*i);
                         if ( v->getFilter() != NULL ) {
                              error_print("WARNING:  Function %s called with a filter argument that conflicts with internal use.", getName());
                         } else {
                              v->setFilter((char*)outer->getFilter());
                              params.filterResets.push_back(v);
                         }
                    }

               }
          }
     }

     if ( !getDests()->isNull() ) {
          size_t cCount = getDests()->childCount();
          for ( size_t i = 0 ; i < cCount ; i++ ) {
               ASTVar *outer = out->getVar(i);
               ASTVar *inner = getDests()->getVar(i);

               StreamVar *svOutter = outer->getStreamVar();
               StreamVar *svInner = inner->getStreamVar();

               params.outList.push_back(svInner->node);
               svInner->node = svOutter->node;
          }
     }

     params.success = true;

     return params;
}


void ASTFuncDecl::resetParameters(ASTFuncDecl::paramList_t& params)
{

     if ( !getSources()->isNull() ) {
          size_t cCount = getSources()->childCount();
          for ( size_t i = 0 ; i < cCount ; i++ ) {
               ASTVar *inner = getSources()->getVar(i);

               StreamVar *svIn = inner->getStreamVar();

               svIn->node = params.inList[i];
          }
     }

     if ( !getDests()->isNull() ) {
          size_t cCount = getDests()->childCount();
          for ( size_t i = 0 ; i < cCount ; i++ ) {
               ASTVar *inner = getDests()->getVar(i);

               StreamVar *svIn = inner->getStreamVar();

               svIn->node = params.outList[i];
          }
     }

     for ( std::vector<ASTVar*>::iterator i = params.filterResets.begin() ; i != params.filterResets.end() ; ++i ) {
          ASTVar *var = (*i);
          var->setFilter(NULL);
     }

     for ( node_iterator i = getBody()->getChildren().begin() ; i != getBody()->getChildren().end() ; ++i ) {
          ASTPipeline *pipe = dynamic_cast<ASTPipeline*>(*i);
          if ( pipe ) {
               if ( !pipe->getSources()->isNull() ) {
                    pipe->getSources()->deregisterStream();
               }
               if ( !pipe->getSink()->isNull() ) {
                    pipe->getSink()->deregisterStream();
               }
          } else {
               ASTFuncCall *call = dynamic_cast<ASTFuncCall*>(*i);
               if ( call ) {
                    if ( !call->getSources()->isNull() ) {
                         call->getSources()->deregisterStream();
                    }
                    if ( !call->getDests()->isNull() ) {
                         call->getDests()->deregisterStream();
                    }
               }
          }
     }
     recursionDetect = false;
}



void* ASTNode::dispatch(ASTDispatch &target)          { return target.processNode(*this); }
void* ASTNULL::dispatch(ASTDispatch &target)          { return target.processNode(*this); }
void* ASTStatementList::dispatch(ASTDispatch &target) { return target.processNode(*this); }
void* ASTKidDef::dispatch(ASTDispatch &target)        { return target.processNode(*this); }
void* ASTKidList::dispatch(ASTDispatch &target)       { return target.processNode(*this); }
void* ASTVar::dispatch(ASTDispatch &target)           { return target.processNode(*this); }
void* ASTVarList::dispatch(ASTDispatch &target)       { return target.processNode(*this); }
void* ASTThreadDecl::dispatch(ASTDispatch &target)    { return target.processNode(*this); }
void* ASTFuncDecl::dispatch(ASTDispatch &target)      { return target.processNode(*this); }
void* ASTFuncCall::dispatch(ASTDispatch &target)      { return target.processNode(*this); }
void* ASTPipeline::dispatch(ASTDispatch &target)      { return target.processNode(*this); }

uint32_t ASTFuncDecl::funcCount = 0;
