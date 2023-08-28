import Container from 'react-bootstrap/Container';
import Navbar from 'react-bootstrap/Navbar';
import logo from '../assets/header-logo.png'
import '../styles/styles.css'

function TopNavbar() {
    return (
        <>
            <Navbar className="top-navbar">
                <Container className='navbar-inner'>
                    <Navbar.Brand href='#'>
                        <img 
                            src={logo}
                            height="30px"
                            className="d-flex  align-top"
                            alt="React Bootstrap logo"
                        />
                    </Navbar.Brand>
                </Container>
            </Navbar>
        </>
    );
}

export default TopNavbar;