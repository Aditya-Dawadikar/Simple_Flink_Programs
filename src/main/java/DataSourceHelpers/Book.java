package DataSourceHelpers;

public class Book {

    public int id;
    public String title;
    public String authors;
    public int year;

    public Book(){
        this.id = 0;
        this.title="";
        this.authors = "";
        this.year = 0;
    }

    public Book(int id, String title, String authors, int year) {
        this.id = id;
        this.title = title;
        this.authors = authors;
        this.year = year;
    }

    public int getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getAuthors() {
        return authors;
    }

    public Integer getYear() {
        return year;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setAuthors(String authors) {
        this.authors = authors;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

}
