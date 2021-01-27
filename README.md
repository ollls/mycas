# Recursive, CAS only, lock-free, generic, dynamically resized implementation of Skip List. Ordering of elements is supported by Scala’s implicit Ordered type.

Update: Jan, 2021<br>
Please, check test cases ( sbt test ) and play with FACTOR, FACTOR means the number of linear lookup attempts in a single linked lists. 
The whole structure can be made flat ( with 1 or 3 layers ) with factor around 1000. 
Stability is awesome, no matter how many conflicting threads attacking the structure, it maintains all the properties and balance.

Just to explain in a simple way: This is universal binary search tree, with linear lookups configured by FACTOR, 
which can be accessed by unlimited number of threads and it uses no locks. ( so called lock-free ).

It re-arranges itself according the FACTOR on add or remove.

# Disclaimer.
This is a fixed layout skip list, unlike most popular implementation which is a probabilistic skip list. 
There is a price in performance to maintain strict fixed layout of all the layers. It rearranges elements upon addition of a new element and there might be significant performance penalty comparing to probabilistic skip list implementation. On the other hand fixed sorted data always available directly for read/search operations and it won't require any specialized iterrators. 
Main goal of the project  ( mostly educational ) is to apply recursive algoritms in scala for mutlti-dimensional lock-free data structures like SkipList consisting out of a layers of LazyLists.
Project was extensively tested on 6 and 12 core machines with special attention to corner cases, like simultaneous addition and removal of neighbor elements.
It is used for PROD business customer supporting smart quick search by agregated silos of keys pointing to the same record. 


## Preface.

sbt - scala build tool used to build project.

- Run tests:           *sbt test*
- Compile the project: *sbt compile*
- Run project:         *sbt run*

## Introduction.
The implementation was inspired by the chapter on a skip list from the book [1] *"The Art of Multiprocessor Programming" by Maurice Herlihy and Nir Shavit.*  Also, [2] *"Functional Programming in Scala" by Paul Chiusano and Runar Bjarnason* refers to neat implementation of Actor.scala in scalaz library to support message passing model, which gave an idea to use AtomicReference for sub-classing a lazy list node. Also, intention was to use tail recursive repeats everywhere when CAS or Compare and Set constraints are not met.
We use concept of Lazy List and and we build our Skip List on top of Lazy List, where Lazy List represents a layer in a Skip List.
Each type of nodes (reference node and data node) implements comon Node interface, with folowing methods:

- *def isFirst* - if first sentinel node.
- *def isLast*  - if last sentinel node, always has a biggest value.
- *def hasRef*  - is that data layer or another reference layer of Skip List?
- *def getRef*  - get bottom node ( decent to adjacent bottom layer of Skip List ).
- *def getOrig* - get reference to original bottom data layer from any node on any layer.

## Core logic.
General idea based on recursive traversal of each skip list’s layer until we reach data layer. 
On the bottom data layer, we perform requested operation: node removal or node addition. Then we unwind the call stack ascending to higher layers, doing additional work, which is either to maintain skip list property [1] by removing non-existing elements/ranges or keeping skip list balanced by removing unnecessary ranges or vice versa: producing new ranges, last two happen when number of items in one range is too big or too small. (see FACTOR parameter).
Skip list is automatically resized, where number of Skip List layers can grow or shrink depending on number of added and removed elements. Also, parameter FACTOR allows you to change maximum number of allowed linear look-ups for a single range.

## Skip List property.
 [1] mentions a necessary property for Skip Lists implementation: "Higher-level lists are always contained in lower-level lists."
To support that property, we deviated from original suggested approach based on the locks and used "CAS by reference mechanism". In recursive algorithm to remove a node we first reach the bottom data layer and atomically mark the node as deleted then we unwind recursion and delete higher level references. There we don’t use lock to maintain skip list property, instead each node keeps extra reference: "orig" to original node, in that case to maintain skip level property it would be enough to validate original bottom layer node and exclude all the higher-level references as if there were marked too. This approach makes the whole 2-dimensional structure atomically lazy and dependent on a single marked bit of bottom layer. 
Another problem happens when we try to remove items from higher layers, we need this to merge skip list ranges which are no longer needed. This introduces interesting side effect, basically skip list itself requests a removal of the node while original node at the bottom data layer remains present. In that case we cannot maintain Skip List property by looking at the marked bit of original data node. 
To solve this, it will be enough to prevent recursive descent to the range which is no longer valid (was recently merged). In that case, we validate a marking bit for next bottom reference as well (ref).  This somewhat a partial enforcement of skip level property, but it successfully prevents recursive descents of top level requests to non-existing nodes which temporary violates skip level property, while parallel recursive ascent finishes removal of no longer valid nodes on all the layers. 
Also, interesting point is that very likely checking of original node’s marking bit is not necessary in that case. But this still maybe useful to prevent long recursive descent to the node which already been deleted.  This approach is somewhat unique, and it allows to create reliable skip list based only CAS primitives.

## Recursive traversal, more details.
The idea behind a recursive algorithm is simple, for example, add() operation starts from top reference which represents a top layer. This becomes an entry point to our skip list. It is protected with AtomicReference to avoid simultaneous add layer/remove layer operations.   findClosestValue() works from the top layer and returns closest element to the value we want to add. 
As a result: the closest value along with its next element in the same list/layer gives us a search range: (from, to] for the bottom layer.  So, now we have a new narrowed search range and we reapply the same add() function one more time. To break the recursion, before we reapply add(), we check if the (from, to) range is a last layer with actual data and if yes, we use linear findClosesValue() to find a Node where we want to insert new element. 
Our skip list always has at least one reference layer, it can be empty with (_first and _last) sentinel nodes.
The usage of sentinel nodes is incredibly useful technique which allows to avoid all the corner case processing. 

## Use cases, examples.

### Basic ops.

    def add(a: A): Boolean
    def remove(a: A): Boolean
    def count(): Int
    def find( a : A  ) : Boolean 
    def findClosestLesser( a : A ) : Option[A]

### Sorted set of Strings.

    val SI = new SkipList[String]

### Sorted set of integer values

    val SI = new SkipList[Int]

### Dictionary: key and value pair.

    val SI = new SkipList[ValuePair[String, String]]

### Iterrating over, instant direct access to nodes.

      def foreach( From :   A,
                   p :  A => Unit,
                   While :  A => Boolean,
                   Filter : A => Boolean = (_) => true)
             
             
- From   - starts from that value.
- p      - call that function or lamda passing each element as a prameter.
- While  - stop calling p when "While" lambda returns false.
- Filter - not to call "p" or exclude some elements from processing but proceed until "While" returns false.

### Project current search result range to Scala's lazy stream.

Prints first 7 entries which starts from capital N.

Explanation on: "new StringIgnoreCaseValuePair( "N", new AtomicInteger(0) )"
This is a backend implementation.
We don't have separate key and value objects. Skip list operates with sigle entities which implements Ordered interface.
Each find() request requires complete A type to be able to search accross list of A. Simple, wraper can be created to build that A type, from key and value pairs. For StringIgnoreCaseValuePair() second parameter is not used, can be null as well.

       SL.toStream( new StringIgnoreCaseValuePair( "N", new AtomicInteger(0) )).take( 7 ).foreach( c => println( c.key ) ) 

             
### Print layers of skiplist and count elements on each layer


SI.debug_print_layers()

Example of output:

FACTOR = 5

    *** Layers ***
    Layer 0 - 2
    Layer 1 - 15
    Layer 2 - 82
    Layer 3 - 414
    Layer 4 - 2119
    Layer 5 - 10679
    Layer 6 - 53486
    Layer 7 - 267672
    Bottom - 2000000
    
FACTOR = 7    

    *** Layers ***
    Layer 0 - 9
    Layer 1 - 72
    Layer 2 - 514
    Layer 3 - 3838
    Layer 4 - 27232
    Layer 5 - 191006
    Bottom - 2000000
    
    
FACTOR = 15    
    
    *** Layers ***
    Layer 0 - 24
    Layer 1 - 378
    Layer 2 - 5890
    Layer 3 - 89552
    Bottom - 2000000
    
 
### Tests.

Project requires more testing, but initial results were very positive, 10 000 000 records were added and removed by 6 parallel threads (tests were on AMD hexacore processor). It works properly with adjacent node removals and maintain proper skip list structure at all times. Though, massive parallel deletes of adjacent nodes provoke sensible delays due to constant conflicts during skip list rearrangement.  This likely can be addressed. 
 



### Word Count Example

Please, see example of paralel word count ran on a big file. 
Originaly it was Bible from http://www.gutenberg.org
mycas/src/main/scala/example/WordCount.scala

It splits file into small pieces, each thread processing it's own part.
Results are gathered in our skip list.


      val dt           : Long = size / THREADS
      var start_offset : Long = 0;


     do {
         val offset = start_offset //copy for lazy eval in the thread, offset inside of lazy block passed to exec()
         exec(es) ( file_task( SL, MY_FILE_PATH, offset, dt, endSignal ) )
         start_offset = start_offset + dt
      } while( start_offset <= size - dt )


To inspect results, we create another Map to be able to see number of occurrences for the actual word. We use "reveresed" to keep the biggest number of occurrences first.

      val SL_REV = new SkipList[MultiValuePairReversed[Int, String]]()

      SL.foreachFrom( SL.head,
      c => {
        SL_REV.add( new MultiValuePairReversed( c.value.get(), c.key ) )
      } )

      //Print first 27
      var counter = 0;
      SL_REV.foreach( c => println(  "%-10s %s".format( c.value, c.key ) ),
                    c =>  {
                      if ( counter > 12 ) false
                      else { counter = counter + 1; true }
                    }) 


Explanation on exec(): start a new thread call:
exec() wraper on lazy/unevalueated block, example of block where 

       T is Int {  val i1 = 10; val i2 = 12; ii + i2  }
       
if we write  (r : T ), T will be evaluated immediately and value 22 will be passed.
if we write ( r :=> T ), in our case is r :=> Int, block will be wrapped up into function ( thunk ) and referred by pointer.
It will be evaluated only when needed, which is inside Callable::call() method.
This called a lazy evaluation. It is nothing but automatic wrapper function which returns value when needed, unless lazy      keyword used again.  


     def exec[T]( es: ExecutorService)(r: => T) : Future[T] = es.submit(new Callable[T] { def call = r })


### Flexible Maps backed by SkipList.

Generic interface for SkipList[A] lets you to choose more complex type then regular Scala's primitive Int or String. At this moment there is no special interface to support Map or Dictionary backed by SkipList. It turned out that using simple ValuePair class which extends Scala's Ordered type with redefined compare() method allows to create a whole set of convinent maps adjusted to specific needs. Special care was taken to avoid direct operations: "==" and "!=" in the Skip List code, since those don't rely on Ordered interface.
Basic map, sorted by primary key.


    class ValuePair[A, B] ( val key : A, var value : B)(implicit ord: A => Ordered[A]) extends Ordered[ValuePair[A,B]] {
      override def compare(that: ValuePair[A,B] ): Int = {
           key.compare(that.key)
      } 
    }


Reversed map, where key is ordered from reversed order from z to a.

    class ReversedValuePair[A, B] ( val key : A, var value : B)(implicit ord: A => Ordered[A]) extends Ordered[ReversedValuePair[A,B]] {
      override def compare(that: ReversedValuePair[A,B] ): Int = {
            key.compare(that.key) * -1

     }
    }
    
    
Basic map where letter case is preserved but not used for key ordering.

         class StringIgnoreCaseValuePair[B] ( val key : String, var value : B) extends Ordered[StringIgnoreCaseValuePair[B]]
         
Map with non-unique keys, it considers second element of the pair: value in comparison as well.

    class MultiValuePair[A, B ] ( val key : A, var value : B)(implicit ord: A => Ordered[A]) extends Ordered[MultiValuePair[A,B]]


To delete element full combination of key and value must be provided. There will be no way to add duplicate combination of key and value. This is tremendously useful functionality which comes almost at no cost. As an example: consider quick as you type search on people's first name.
simple statement:

        forEachFrom( "John", c => println( c.value ) )
        
It prints all the people whose name starts with John, assuming that value is a String with family name of the person.


Map with non-unique keys but in reversed order.

    class MultiValuePairReversed[A, B ] ( val key : A, var value : B)(implicit ord: A => Ordered[A]) 
                    extends Ordered[MultiValuePairReversed[A,B]]
The good example for this one is a paralel word counting, where all the words sorted by number of occurencies from the highest to lowest numbers.
Here is how to type first 27 most popular words in the text.

Where c.value is number of ocurrencies and c.key the actual word.


          SL_REV.foreach( c => println( c.value + "  " + c.key ),
                  c =>  {
                    if ( counter > 27 ) false
                    else { counter = counter + 1; true }
                  })
    
