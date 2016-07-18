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
#include "graph.tab.hh"
#include "wsqueue.h"
#include "parse_graph.h"


extern int pg_parse_graph(ASTNode *root, SymbolTable *symTab, nhqueue_t *files, const char *str);


class GraphBuilder {
     static GraphBuilder *instance;

     nhqueue_t *files;
     char *cmdString;

     ASTNode *astRoot;
     SymbolTable *symtab;

     GraphBuilder() :
          files(queue_init()), cmdString(NULL),
          astRoot(new ASTNode()), symtab(new SymbolTable())
     {
     }

     ~GraphBuilder()
     {
          char *x = NULL;
          while ( (x = (char*)queue_remove(files)) != NULL ) free(x);
          queue_exit(files);
          if ( cmdString ) free(cmdString);
          delete astRoot;
          delete symtab;
     }


     /* For debugging */
     struct ASTPrinter : public ASTWalker {
          virtual void operator()(ASTNode *n) {
               if ( !n->isNull() ) {
                    for ( uint32_t i = 0 ; i < depth ; i++ ) printf("  ");
                    printf("%s\n", n->asString());
               }
          }
     };


     struct Dispatcher : public ASTDispatch {
          parse_graph_t *pg;
          SymbolTable *symtab;
          const bool debug;
          uint32_t errors;
          unsigned long long thread_context;
          bool twoD_placement;
          std::map<std::string, int> procCounts;
          std::map<std::string, int> funcCounts;
          uint32_t tempStreamCount;

          Dispatcher(parse_graph_t *pg) : ASTDispatch(), pg(pg), debug(false)
          {
               errors = 0;
               thread_context = -1;
               twoD_placement = false;
               tempStreamCount = 0;
               symtab = getGB()->symtab;
               symtab->setParseGraph(pg);
          }

          void* processNode(ASTNode &node)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: processNode(ASTNode)\n");
               for ( std::deque<ASTNode*>::iterator i = node.getChildren().begin() ; i != node.getChildren().end() ; ++i ) {
                    (*i)->dispatch(*this);
               }
               return NULL;
          }


          void* processNode(ASTStatementList &node)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: processNode(ASTStatementList)\n");
               /* For whatever reason, the old parser ran these in reverse order.
                * We shall do the same, for the sake of consistency. */
               for ( std::deque<ASTNode*>::reverse_iterator i = node.getChildren().rbegin() ; i != node.getChildren().rend() ; ++i ) {
                    (*i)->dispatch(*this);
               }
               return NULL;
          }


          void* processNode(ASTThreadDecl &node)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: processNode(ASTThreadDecl)\n");
               thread_context = node.tid;
               twoD_placement = node.twoD;

               node.getBody()->dispatch(*this);

               thread_context = -1;
               twoD_placement = false;
               return NULL;
          }


          void* processNode(ASTFuncDecl &node)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: processNode(ASTFuncDecl)\n");

               return NULL;
          }


          void* processNode(ASTFuncCall &node)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: processNode(ASTFuncCall)\n");
               ASTFuncDecl *decl = symtab->findFunction(node.getName());
               if ( !decl ) {
                    error_print("Unable to find function declaration for %s", node.getName());
                    errors++;
                    return NULL;
               }

               uint32_t version = funcCounts[node.getName()]++;

               ASTFuncDecl::paramList_t oldParams = decl->setupParameters(node.getSources(), node.getDests(), version, pg);

               if ( !oldParams.success ) {
                    error_print("Failed to set up parameters for function call.");
                    errors++;
                    return NULL;
               }

               SymbolTable *oldsymtab = symtab;
               symtab = decl->getSymbolTable();

               decl->getBody()->dispatch(*this);

               symtab = oldsymtab;
               decl->resetParameters(oldParams);


               return NULL;
          }


          void createVarProcEdge(ASTVar *var, ASTKidDef *kid)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: CreateVarProcEdge\n");
               parse_edge_t *edge = (parse_edge_t*)calloc(1, sizeof(parse_edge_t));
               if ( !edge ) { errors++; return; }

               StreamVar *svar = symtab->findStreamVariable(var);
               if ( !svar ) {
                    error_print("Unable to find stream Variable %s", var->getFullName());
                    errors++;
                    return;
               }

               edge->edgetype = PARSE_EDGE_TYPE_VARPROC;
               edge->src = svar->getParseNode();
               edge->src_label = var->getFilter();
               edge->port = var->getTargetPort();
               if ( kid->getSourcePort() )
                    edge->port = kid->getSourcePort();
               edge->dst = kid->getParseNode();
               edge->thread_context = thread_context;
               edge->thread_trans = (kid->getInPipeType() == ASTKidDef::DOUBLEPIPE);
               queue_add(pg->edges, edge);
          }


          void createProcVarEdge(ASTKidDef *kid, ASTVar *var)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: CreateProcVarEdge\n");
               parse_edge_t *edge = (parse_edge_t*)calloc(1, sizeof(parse_edge_t));
               if ( !edge ) { errors++; return; }

               StreamVar *svar = symtab->findStreamVariable(var);
               if ( !svar ) {
                    error_print("Unable to find stream Variable %s", var->getFullName());
                    errors++;
                    return;
               }

               edge->edgetype = PARSE_EDGE_TYPE_PROCVAR;
               edge->src = kid->getParseNode();
               edge->dst = svar->getParseNode();
               edge->thread_context = -1;
               queue_add(pg->edges, edge);
          }


          void createProcProcEdge(ASTKidDef *kid0, ASTKidDef *kid1) {
               parse_node_proc_t *srcNode = kid0->getParseNode();
               parse_node_proc_t *dstNode = kid1->getParseNode();

               parse_edge_t *edge = (parse_edge_t*)calloc(1, sizeof(parse_edge_t));
               if ( !edge ) { errors++; return; }

               edge->edgetype = PARSE_EDGE_TYPE_PROCPROC;
               edge->src = srcNode;
               edge->dst = dstNode;
               edge->port = kid1->getSourcePort();
               edge->src_label = NULL;
               edge->thread_trans = (kid1->getInPipeType() == ASTKidDef::DOUBLEPIPE);
               edge->thread_context = thread_context;
               edge->twoD_placement = twoD_placement;

               queue_add(pg->edges, edge);
          }



          void processBundledSource(ASTVar *var, ASTKidDef *target)
          {
               /* We have @$foo | kid_def ...
                * Need to basically create a new, temporary, pipeline of the form:
                * $var | unbundle -> $tempStreamXXXX;
                * $tempStreamXXXX | target ...
                */
               char tempStreamName[24];
               snprintf(tempStreamName, 24, "tempStream%08x", tempStreamCount++);

               ASTKidDef *unbun = new ASTKidDef(strdup("unbundle"));
               unbun->setInPipeType(ASTKidDef::PIPE);
               unbun->dispatch(*this);
               createVarProcEdge(var, unbun);

               ASTVar *sinkVar = new ASTVar(strdup(tempStreamName), symtab);
               sinkVar->setSink(unbun);
               createProcVarEdge(unbun, sinkVar);

               ASTVar *sourceVar = new ASTVar(strdup(tempStreamName), symtab);
               sourceVar->setSource(target);
               createVarProcEdge(sourceVar, target);

               sinkVar->deregisterStream();
               delete sinkVar;
               delete unbun;

               sourceVar->deregisterStream();
               delete sourceVar;
          }


          void* processNode(ASTPipeline &node)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: processNode(ASTPipeline)\n");
               /* First, build the PROCPROC edges */
               node.getPipeline()->dispatch(*this);

               /* Handle sources */
               ASTVarList *sources = node.getSources();
               if ( !sources->isNull() ) {
                    ASTKidDef *tgt = static_cast<ASTKidDef*>(node.getPipeline()->getFirstChild());
                    for ( size_t i = 0 ; i < sources->childCount() ; i++ ) {
                         ASTVar *var = sources->getVar(i);

                         if ( var->isBundled() ) {
                              processBundledSource(var, tgt);
                         } else {
                              createVarProcEdge(var, tgt);
                         }
                    }
               }


               /* Handle sinks */
               ASTVar *sinkVar = node.getSink();
               if ( !sinkVar->isNull() ) {
                    ASTKidDef *lastKid = static_cast<ASTKidDef*>(node.getPipeline()->getLastChild());

                    /* Handle Bundle target */
                    if ( sinkVar->isBundled() ) {
                         ASTKidDef *bun = new ASTKidDef(strdup("bundle"));
                         bun->setInPipeType(ASTKidDef::PIPE);
                         bun->dispatch(*this);
                         createProcProcEdge(lastKid, bun);

                         lastKid = bun;
                    }

                    createProcVarEdge(lastKid, sinkVar);

                    /* Don't leak the memory... Delete temporary kid */
                    if ( sinkVar->isBundled() ) {
                         /* We pointed lastKid to our temporary Bundle kid */
                         delete lastKid;
                    }
               }

               return NULL;
          }


          void* processNode(ASTKidList &node)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: processNode(ASTKidList)\n");
               size_t nkids = node.childCount();

               ASTKidDef *prevKid = node.getKid(0);
               prevKid->dispatch(*this);
               for ( size_t i = 1 ; i < nkids ; i++ ) {
                    ASTKidDef *kid = node.getKid(i);
                    parse_node_proc_t *kidNode = static_cast<parse_node_proc_t*>(kid->dispatch(*this));

                    if ( !kidNode ) { errors++; return NULL; }

                    createProcProcEdge(prevKid, kid);

                    prevKid = kid;

               }
               return NULL;
          }


          void* processNode(ASTKidDef &node)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: processNode(ASTKidDef)\n");

               uint32_t version = procCounts[node.getKidName()]++;
               char mungedName[64];
               snprintf(mungedName, 63, "%s.%d", node.getKidName(), version);

               parse_node_proc_t *proc = (parse_node_proc_t*)listhash_find_attach(pg->procs, mungedName, strlen(mungedName));


               if ( debug) fprintf(stderr, "Creating entry for proc %s\n", mungedName);

               node.setParseNode(proc);

               proc->name = strdup(node.getKidName());
               proc->version = version;
               proc->thread_context = thread_context;
               proc->argv = (char**)calloc(1, sizeof(char*) * node.getTokens().size());
               proc->argc = 0;
               for ( std::list<char*>::const_iterator i = node.getTokens().begin() ; i != node.getTokens().end() ; ++i ) {
                    proc->argv[proc->argc++] = strdup(*i);
               }

               // Check if name matches mimo source or sink
               if (check_mimo_source(pg->mimo, proc->name)) {
                    proc->mimo_source = 1;
               }
               else if (check_mimo_sink(pg->mimo, proc->name)) {
                    proc->mimo_sink = 1;
               }

               if ( node.getInPipeType() == ASTKidDef::DOUBLEPIPE ) {
                    mimo_using_deprecated(pg->mimo, "Doublepipe in config graph");
               }

               return proc;
          }


          void* processNode(ASTVarList &node)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: processNode(ASTVarList)\n");
               /* Unused */
               return NULL;
          }


          void* processNode(ASTVar &node)
          {
               if ( debug ) fprintf(stderr, "GB Dispatch: processNode(ASTVar)\n");
               /* Unused */
               return NULL;
          }

          bool success() {
               return (errors == 0);
          }
     };


public:
     static GraphBuilder* getGB()
     {
          if ( NULL == instance ) instance = new GraphBuilder();
          return instance;
     }


     static void freeGB()
     {
          if ( NULL != instance ) delete instance;
          instance = NULL;
     }


     void add_file(const char *fname)
     {
          queue_add(files, strdup(fname));
     }


     void set_cmdstring(const char *str)
     {
          if ( cmdString ) free(cmdString);
          cmdString = strdup(str);
     }


     int parse(void)
     {
          if ( !cmdString && !queue_size(files) ) {
               error_print("Nothing to parse!");
               return 0;
          }

          if ( !pg_parse_graph(astRoot, symtab, files, cmdString) ) {
               return 0;
          }

          free(cmdString); cmdString = NULL;

          /* Debugging */
#if 0
          ASTPrinter printer;
          astRoot->walk(printer);
#endif

          return 1;
     }


     parse_graph_t* buildGraph(mimo_t *mimo)
     {
          if ( !astRoot->hasChildren() ) {
               error_print("buildGraph:  No graph");
               return NULL;
          }

          /* Init the parse graph */
          parse_graph_t *pg = (parse_graph_t*)calloc(1, sizeof(parse_graph_t));
          if ( !pg ) {
               error_print("Failed to calloc parsegraph");
               return NULL;
          }

          pg->mimo = mimo;
		pg->edges = queue_init();
		pg->vars = listhash_create(PARSE_GRAPH_MAXLIST, sizeof(parse_node_var_t));
		pg->procs = listhash_create(PARSE_GRAPH_MAXLIST, sizeof(parse_node_proc_t));

          Dispatcher builder(pg);
          astRoot->dispatch(builder);
          if ( !builder.success() ) {
               error_print("Failed to build graph from AST");

               listhash_destroy(pg->procs);
               listhash_destroy(pg->vars);
               queue_exit(pg->edges);
               free(pg);
               pg = NULL;
          }

          //wsprint_graph_dot(pg, stdout);

          return pg;
     }

};

GraphBuilder* GraphBuilder::instance;




extern "C" {

void pg_add_file(const char *fname)
{
     GraphBuilder::getGB()->add_file(fname);
}

void pg_set_cmdstring(const char *str)
{
     GraphBuilder::getGB()->set_cmdstring(str);
}


int pg_parse(void)
{
     return GraphBuilder::getGB()->parse();
}


parse_graph_t*  pg_buildGraph(mimo_t *mimo)
{
     return GraphBuilder::getGB()->buildGraph(mimo);
}


void pg_cleanup(void)
{
     GraphBuilder::freeGB();
}

}



static void buildThreadMap(void *vproc, void *vmap)
{
     parse_node_proc_t *proc = (parse_node_proc_t*)vproc;
     std::map<int, std::vector<std::string> > &map = *(std::map<int, std::vector<std::string> >*)vmap;

     if ( proc->thread_context >=0 ) {
          char buf[128];
          sprintf(buf, "%s.%u", proc->name, proc->version);
          map[proc->thread_context].push_back(buf);
     }
}


void wsprint_graph_dot(parse_graph_t * pg, FILE * fp) {
	parse_edge_t * edge;
	q_node_t * qnode;
	parse_node_proc_t * proc;
	parse_node_var_t * var;
	int cnt = 0;


	fprintf(stderr, "printing dot graph\n");
  
	if (!pg || !fp) {
		error_print("one or more NULL argument pointers");
		return;
	}

	fprintf(fp, "digraph G {\n");
	//fprintf(fp, "rankdir=LR;\n");
	fprintf(fp, "rankdir=TD;\n");
	if (!pg->edges) {
		fprintf(stderr, "no edges in graph\n");
	}
	for (qnode = pg->edges->head; qnode; qnode = qnode->next) {
		edge = (parse_edge_t *)qnode->data;
		cnt++;

		switch (edge->edgetype >> 4) {
		case PARSE_NODE_TYPE_PROC:
			proc = (parse_node_proc_t *)edge->src;
			fprintf(fp, "\"%s.%u\" -> ", proc->name, proc->version );
			break;
		case PARSE_NODE_TYPE_VAR:
			var = (parse_node_var_t *)edge->src;
			fprintf(fp, "\"%s\" -> ", var->name);
			break;
		}

		switch (edge->edgetype & 0x01) {
		case PARSE_NODE_TYPE_PROC:
			proc = (parse_node_proc_t *)edge->dst;
			fprintf(fp, "\"%s.%u\"", proc->name, proc->version );
			break;
		case PARSE_NODE_TYPE_VAR:
			var = (parse_node_var_t *)edge->dst;
			fprintf(fp, "\"%s\"", var->name);
			break;
		}
		if (edge->src_label || edge->port || edge->thread_trans) {
			fprintf(fp, "[label=\"%s;%s%s\"]",
				   edge->src_label ? edge->src_label : "",
                       edge->port ? edge->port : "",
				   edge->thread_trans ? "THREAD":"");
		}
		fprintf(fp, ";\n");

	}

     std::map<int, std::vector<std::string> > threadMap;
     listhash_scour(pg->procs, buildThreadMap, &threadMap);
     for ( std::map<int, std::vector<std::string> >::iterator i = threadMap.begin() ; i != threadMap.end() ; ++i ) {
          int tid = i->first;
          std::vector<std::string> &procs = i->second;
          fprintf(fp, "\n  subgraph cluster_t%d {\n", tid);
          fprintf(fp, "      label = \"thread %d\";\n", tid);
          for ( std::vector<std::string>::iterator j = procs.begin() ; j != procs.end() ; ++j ) {
               fprintf(fp, "      \"%s\";\n", j->c_str());
          }
          fprintf(fp, "  }\n");

     }


	fprintf(fp, "}\n");
	fprintf(stderr, "finishing graph\n");
	fclose(fp);
}


#if 0
// extern int pgdebug;
int main(int argc, char **argv) {

//     pgdebug=1;

     nhqueue_t *file_queue = queue_init();
     queue_add(file_queue, strdup(argv[1]));

     ASTNode *root = new ASTNode();
     SymbolTable *symtab = new SymbolTable();

     if ( pg_parse_graph(root, symtab, file_queue, NULL) ) {
          ASTPrinter printer;
          root->walk(printer);
     }
     queue_exit(file_queue);

//     buildGraph(root);

     delete root;
     delete symtab;

     return 0;
}
#endif
