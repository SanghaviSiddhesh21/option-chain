import React, { useState, useEffect } from 'react';
import NavDropdown from 'react-bootstrap/NavDropdown';
import { useDispatch } from 'react-redux'
import { setSymbolAndExpiry } from '../features/appSlice';
import OptionTable from './table';
import '../styles/styles.css';
import { baseUrl } from '../constants';

const Dropdown = () => {
    const symbols = [
        "ALLBANKS",
        "FINANCIALS",
        "MAINIDX",
        "MIDCAP"
    ]
    const dispatch = useDispatch()
    const [selectedIndex, setSelectedIndex] = useState('');
    const [selectedExpiry, setSelectedExpiry] = useState('');
    const [selectedStrike, setSelectedStrike] = useState('');
    const [selectedSymbol, setSelectedSymbol] = useState('');
    const [selectedSymbolStrikePriceSection, setSelectedSymbolStrikePriceSection] = useState('');
    const [expiry, setExpiry] = useState('')
    const [strikePrice, setStrikePrice] = useState('')
    const [symbolAndDate, setsymbolAndDate] = useState('')
    const [data, setData] = useState([]);
    const apiUrl = `https://caf7-2401-4900-57eb-86db-2c9d-a1b-d4de-4106.in.ngrok.io/api/mount_options`; // Update the API endpoint here
    /* 
    /api/symbol_date_option / <string:symbol>+<string:expiry>
    /api/symbol_price_option/<string:symbol>+<string:price> 
    */
    useEffect(() => {
        fetchData();
        handleSetSymbolAndDate()
    }, [selectedExpiry, selectedSymbol]);
    const fetchExpiryDate = async (sym) => {
        const response = await fetch(`${baseUrl}/symbol_option/${sym}`)
        const data = await response.json()
        setExpiry(data.expiry);
        setSelectedExpiry('')
    }
    const fetchStrikePrice = async (sym) => {
        const response = await fetch(`${baseUrl}/symbol_option/${sym}`)
        const data = await response.json()
        console.log("🚀 ~ file: dropdown.js:44 ~ fetchStrikePrice ~ data   ~~~  :", data)
        setStrikePrice(data.strike_price);
        setSelectedStrike('')
    }
    const fetchData = async () => {
        try {
            const response = await fetch(apiUrl);
            const jsonData = await response.json();
            setData(jsonData);
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    };

    const handleSetSymbolAndDate = () => {
        console.log(typeof (selectedExpiry));
        console.log(typeof (selectedSymbol));
        if ((selectedExpiry && selectedSymbol) !== '') {
            dispatch(setSymbolAndExpiry(`${selectedSymbol}+` + selectedExpiry))
        }
    }

    const handleIndexChange = (index) => {
        setSelectedIndex(index);
    };

    const handleExpiryChange = (expiry) => {
        setSelectedExpiry(expiry);
        setSelectedIndex(''); // Reset selectedIndex when expiry changes

    };

    const handleStrikeChange = (strike) => {
        setSelectedStrike(strike);
        setSelectedIndex(''); // Reset selectedIndex when strike changes
    };

    const handleSymbolChange = (symbol) => {
        setSelectedSymbol(symbol);
        fetchExpiryDate(symbol)
        setSelectedIndex(''); // Reset selectedIndex when symbol changes
    };
    const handleSymbolChangeStrikePriceSection = (symbol) => {
        setSelectedSymbolStrikePriceSection(symbol);
        fetchStrikePrice(symbol)
        setSelectedIndex(''); // Reset selectedIndex when symbol changes
    };

    //symbolAndExpiry
    return (
        <div>
            <table className="">
                <tr className="" >
                    <td style={{ borderWidth: 1, }}>
                        <tr className="">
                            <td>select symbol</td>

                            <td><NavDropdown
                                className='dropdown-2'
                                title={selectedSymbol || 'Symbol'}
                                onSelect={handleSymbolChange}
                            >
                                {
                                    symbols.map((symbol, index) => (
                                        <NavDropdown.Item key={index} eventKey={symbol}>
                                            {symbol}
                                        </NavDropdown.Item>
                                    ))}
                            </NavDropdown></td>
                        </tr>
                        <tr className="">
                            <td>select Expiry Date</td>
                            <td><NavDropdown
                                className='dropdown-2'
                                title={selectedExpiry || 'Select'}
                                onSelect={handleExpiryChange}
                            >
                                {expiry && expiry.map((expiry, index) => (
                                    <NavDropdown.Item key={index} eventKey={expiry}>
                                        {expiry}
                                    </NavDropdown.Item>
                                ))}
                            </NavDropdown></td>
                        </tr>
                    </td>
                    <td className="" style={{ paddingRight: 10 }}>{" "}</td>
                    <td style={{ borderWidth: 1 }}>
                        <tr className="">
                            <td>select symbol</td>

                            <td><NavDropdown
                                className='dropdown-2'
                                title={selectedSymbolStrikePriceSection || 'Symbol'}
                                onSelect={handleSymbolChangeStrikePriceSection}
                            >
                                {symbols.map((symbol, index) => (
                                    <NavDropdown.Item key={index} eventKey={symbol}>
                                        {symbol}
                                    </NavDropdown.Item>
                                ))}
                            </NavDropdown></td>
                        </tr>
                        <tr className="">
                            <td>select Strike Price</td>
                            <td><NavDropdown
                                className='dropdown-2'
                                title={selectedStrike || 'Strike Price'}
                                onSelect={handleStrikeChange}
                            >
                                {strikePrice &&
                                    strikePrice.map((strike, index) => (
                                        <NavDropdown.Item key={index} eventKey={strike}>
                                            {strike}
                                        </NavDropdown.Item>
                                    ))}
                            </NavDropdown></td>
                        </tr>
                    </td>
                </tr>
            </table>
            {/* <OptionTable selIndex={selectedIndex} selExpiry={selectedExpiry} selStrike={selectedStrike} selSymbol={selectedSymbol}></OptionTable> */}
        </div>
    );
};

export default Dropdown;



