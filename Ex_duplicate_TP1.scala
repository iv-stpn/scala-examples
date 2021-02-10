object Ex_duplicate_TP1 extends App {

    def duplicate[A](l: List[A]): List[A] = l match {
        case h :: tail => List(h, h) ::: duplicate(tail)
        case Nil => Nil
    }

    def duplicate_map[A](l: List[A]): List[A] = l.flatMap(x => List.fill(2)(x))


    println(duplicate(List(1, 2, 3, 4, 5)))
    println(duplicate_map(List(1, 2, 3, 4, 5)))
}
