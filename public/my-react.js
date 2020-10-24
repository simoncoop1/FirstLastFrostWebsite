var React = require('react');



class Observations extends React.Component{
    constructor(props){
        super(props);
    }
    render(){
        return(
            <div>
                <MyComponent/>
                <br/>
                <MyFooter/>
            </div>
        );
    }
}

class MyComponent extends React.Component{
    constructor(props){
        super(props);
    }
    render(){
        return (<h1>OK</h1>);
    }
}

class MyFooter extends React.Component{
    constructor(props){
        super(props);
    }
    render(){
        return (
            <footer className="footer mt-auto py-3">
              <div className="container">
                <span className="text-muted">My Project Footer</span>
              </div>
            </footer>

        );
    }
}
