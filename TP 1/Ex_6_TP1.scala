object Ex_6_TP1 extends App {
    def decode_map(l:List[(Int, String)]):List[String] = l.flatMap(x => List.fill(x._1)(x._2))    
    
    def decode(l:List[(Int, String)]):List[String] = l match {
        case couple :: reminder => List.fill(couple._1)(couple._2) ::: decode(reminder)
        case Nil => Nil
    }

    println(decode(List((4, "a"), (1, "b"), (2, "c"), (2, "a"), (1, "d"), (4, "e"))))
    println(decode_map(List((4, "a"), (1, "b"), (2, "c"), (2, "a"), (1, "d"), (4, "e"))))
    println(duplicate(List(1, 2, 3, 4, 5)))
    println(duplicate_map(List(1, 2, 3, 4, 5)))
}
