# CloudComputing_WordCount2


하둡과 맵리듀스 프레임워크에 대한 기본적인 이해를 하고자 강의 중에 소개한 WordCount
프로그램을 약간 변형한 WordCount2 프로그램을 작성하고자 한다. WordCount2 프로그램의
입력은 WordCount 프로그램과 마찬가지로 단어들이 나열되어 있는 텍스트 파일이다. 예를 들
어, 아래 두 파일이 입력이라고 가정하자. 이들은 src/test/resources 폴더에 저장되어 있다고
가정하자.

WordCount2 프로그램은 이들 파일을 모두 읽고, 이 파일들에 포함되어 있는 모든 word의 개
수를 센 후에, 아래 그림과 같이 빈도 (frequency)가 높은 단어부터 내림차순으로 “빈도와 단
어”를 출력한다.

※ 참고, 고려해야 할 사항
○ MapReduce 프로그램은 입력 폴더내의 모든 파일을 처리한다는 점을 기억하자. ○ 입력 파일의 형식은? key, value 타입은? 파일의 위치는?
○ MapReduce 프레임워크는 key 의 오름차순으로 출력을 한다. ○ Reducer는 몇 개를 써야 하는가?
○ 출력 파일의 형식은? key, value 타입은? 출력 파일의 위치는?
○ 출력 파일이 저장될 폴더는 비어 있어야 하고, 프로그램 실행 후 refresh 해야 보인다. ○ Job Chaining 이 필요한가?
