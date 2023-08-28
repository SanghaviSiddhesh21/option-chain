import { useState } from 'react';
import logo from '../assets/logo.png'
import '../styles/styles.css'
import Banner from './banner';
import Dropdown from './dropdown';
import { DropdownData } from '../data';


function Hero() {

    return (
        <>

            <div className='banner-main'>

                <Banner />
                <div className='banner-button-area'>
                    <div className='dropdown-left'>

                        {/* Render Data based on the classifications that are made */}
                        <Dropdown />
                    </div>
                </div>


            </div>
        </>
    );
}

export default Hero;