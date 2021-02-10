def decode_map(l:List[(Int, String)]):List[String] = l.flatMap(x => List.fill(x._1)(x._2))    
    
def decode(l:List[(Int, String)]):List[String] = l match {
    case couple :: reminder => List.fill(couple._1)(couple._2) ::: decode(reminder)
    case Nil => Nil
}